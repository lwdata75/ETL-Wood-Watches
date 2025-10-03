import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'arbore-snowpipe'}, 
    )


def detect_record_type(record):
    """Detect if record is an order or warranty claim based on fields"""
    if 'order_id' in record and 'customer_id' in record and 'quantity' in record:
        return 'order'
    elif 'claim_id' in record and 'return_date' in record and 'return_reason' in record:
        return 'claim'
    else:
        return 'unknown'


def save_orders_to_snowflake(snow, orders_batch, temp_dir, orders_ingest_manager):
    """Save orders batch to Snowflake using Snowpipe"""
    logging.debug('inserting orders batch to db via Snowpipe')
    
    # Convert orders to DataFrame
    orders_data = []
    for record in orders_batch:
        # Handle quantity as JSON string for VARIANT
        quantity_json = json.dumps(record.get('quantity'))
        orders_data.append([
            record.get('order_id'),
            record.get('customer_id'),
            record.get('product_id'),
            quantity_json,  # VARIANT field
            record.get('order_date'),
            record.get('order_notes')
        ])
    
    pandas_df = pd.DataFrame(orders_data, columns=[
        "ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "QUANTITY", "ORDER_DATE", "ORDER_NOTES"
    ])
    
    # Convert to Parquet and upload
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"orders_{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"
    
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    # Convert Windows path to proper format for Snowflake PUT command
    put_path = out_path.replace('\\', '/')
    snow.cursor().execute("PUT 'file://{0}' @%ARBORE_ORDERS".format(put_path))
    os.unlink(out_path)
    
    # Send to Snowpipe for serverless ingestion
    resp = orders_ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f"Orders response from Snowflake for file {file_name}: {resp['responseCode']}")
    return len(orders_batch)


def save_claims_to_snowflake(snow, claims_batch, temp_dir, claims_ingest_manager):
    """Save warranty claims batch to Snowflake using Snowpipe"""
    logging.debug('inserting claims batch to db via Snowpipe')
    
    # Convert claims to DataFrame
    claims_data = []
    for record in claims_batch:
        claims_data.append([
            record.get('claim_id'),
            record.get('order_id'),
            record.get('product_id'),
            record.get('order_date'),
            record.get('return_date'),
            record.get('return_reason'),
            record.get('severity'),
            record.get('under_warranty')
        ])
    
    pandas_df = pd.DataFrame(claims_data, columns=[
        "CLAIM_ID", "ORDER_ID", "PRODUCT_ID", "ORDER_DATE", "RETURN_DATE", 
        "RETURN_REASON", "SEVERITY", "UNDER_WARRANTY"
    ])
    
    # Convert to Parquet and upload
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"claims_{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"
    
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    # Convert Windows path to proper format for Snowflake PUT command  
    put_path = out_path.replace('\\', '/')
    snow.cursor().execute("PUT 'file://{0}' @%ARBORE_WARRANTY_CLAIMS".format(put_path))
    os.unlink(out_path)
    
    # Send to Snowpipe for serverless ingestion
    resp = claims_ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f"Claims response from Snowflake for file {file_name}: {resp['responseCode']}")
    return len(claims_batch)


def load_json_file_to_snowpipe(filepath, batch_size):
    """Load JSON file and process through Snowpipe"""
    print(f"Loading {filepath} via Snowpipe with batch size {batch_size}...")
    
    # Setup connections and managers
    snow = connect_snow()
    temp_dir = tempfile.TemporaryDirectory()
    
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    
    # Create ingest managers for both pipes
    orders_ingest_manager = SimpleIngestManager(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=host,
        user=os.getenv("SNOWFLAKE_USER"),
        pipe='INGEST.INGEST.ARBORE_ORDERS_PIPE',
        private_key=private_key
    )
    
    claims_ingest_manager = SimpleIngestManager(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=host,
        user=os.getenv("SNOWFLAKE_USER"),
        pipe='INGEST.INGEST.ARBORE_WARRANTY_CLAIMS_PIPE',
        private_key=private_key
    )
    
    # Load JSON data
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        data = [data]
    
    # Separate batches for orders and claims
    orders_batch = []
    claims_batch = []
    orders_processed = 0
    claims_processed = 0
    
    try:
        for record in data:
            record_type = detect_record_type(record)
            
            if record_type == 'order':
                orders_batch.append(record)
                if len(orders_batch) >= batch_size:
                    count = save_orders_to_snowflake(snow, orders_batch, temp_dir, orders_ingest_manager)
                    orders_processed += count
                    orders_batch = []
                    print(f"Processed {orders_processed} orders so far...")
                    
            elif record_type == 'claim':
                claims_batch.append(record)
                if len(claims_batch) >= batch_size:
                    count = save_claims_to_snowflake(snow, claims_batch, temp_dir, claims_ingest_manager)
                    claims_processed += count
                    claims_batch = []
                    print(f"Processed {claims_processed} claims so far...")
        
        # Process remaining records
        if orders_batch:
            count = save_orders_to_snowflake(snow, orders_batch, temp_dir, orders_ingest_manager)
            orders_processed += count
            
        if claims_batch:
            count = save_claims_to_snowflake(snow, claims_batch, temp_dir, claims_ingest_manager)
            claims_processed += count
        
        print(f"✅ Snowpipe processing complete!")
        print(f"📊 Orders processed: {orders_processed}")
        print(f"📊 Claims processed: {claims_processed}")
        print("⏱️  Data will appear in tables within 1-2 minutes (Snowpipe is asynchronous)")
        
    finally:
        temp_dir.cleanup()
        snow.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python py_snowpipe_arbore.py <json_file> <batch_size>")
        print("  python py_snowpipe_arbore.py data_out/orders/orders.json 1000")
        print("  python py_snowpipe_arbore.py data_out/claims/warranty_claims.json 500")
        sys.exit(1)
    
    filepath = sys.argv[1]
    batch_size = int(sys.argv[2])
    
    if not os.path.exists(filepath):
        print(f"❌ Error: File {filepath} not found")
        sys.exit(1)
    
    if not filepath.lower().endswith('.json'):
        print(f"❌ Error: Only JSON files are supported. Got: {filepath}")
        sys.exit(1)
    
    try:
        load_json_file_to_snowpipe(filepath, batch_size)
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Error during Snowpipe processing: {e}")
        sys.exit(1)