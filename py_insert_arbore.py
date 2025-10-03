import os, sys, logging
import json
import snowflake.connector

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle='qmark'


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
        session_parameters={'QUERY_TAG': 'arbore-json-insert'}, 
    )


def detect_record_type(record):
    """Detect if record is an order or warranty claim based on fields"""
    if 'order_id' in record and 'customer_id' in record and 'quantity' in record:
        return 'order'
    elif 'claim_id' in record and 'return_date' in record and 'return_reason' in record:
        return 'claim'
    else:
        return 'unknown'


def save_order_to_snowflake(snow, record):
    """Insert order record into ARBORE_ORDERS table"""
    logging.debug('inserting order record to db')
    
    # Handle quantity as VARIANT (can be number or text)
    quantity_value = record.get('quantity')
    if isinstance(quantity_value, str):
        quantity_json = json.dumps(quantity_value)
    else:
        quantity_json = json.dumps(quantity_value)
    
    row = (
        record.get('order_id'),
        record.get('customer_id'),
        record.get('product_id'),
        quantity_json,
        record.get('order_date'),
        record.get('order_notes')
    )
    
    snow.cursor().execute("""
        INSERT INTO ARBORE_ORDERS 
        (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, ORDER_DATE, ORDER_NOTES) 
        SELECT ?, ?, ?, PARSE_JSON(?), ?, ?
    """, row)
    
    logging.debug(f"inserted order {record.get('order_id')}")


def save_claim_to_snowflake(snow, record):
    """Insert warranty claim record into ARBORE_WARRANTY_CLAIMS table"""
    logging.debug('inserting warranty claim record to db')
    
    row = (
        record.get('claim_id'),
        record.get('order_id'),
        record.get('product_id'),
        record.get('order_date'),
        record.get('return_date'),
        record.get('return_reason'),
        record.get('severity'),
        record.get('under_warranty')
    )
    
    snow.cursor().execute("""
        INSERT INTO ARBORE_WARRANTY_CLAIMS 
        (CLAIM_ID, ORDER_ID, PRODUCT_ID, ORDER_DATE, RETURN_DATE, RETURN_REASON, SEVERITY, UNDER_WARRANTY) 
        SELECT ?, ?, ?, ?, ?, ?, ?, ?
    """, row)
    
    logging.debug(f"inserted claim {record.get('claim_id')}")


def save_to_snowflake(snow, message):
    """Route record to appropriate table based on type"""
    record = json.loads(message)
    record_type = detect_record_type(record)
    
    if record_type == 'order':
        save_order_to_snowflake(snow, record)
    elif record_type == 'claim':
        save_claim_to_snowflake(snow, record)
    else:
        logging.warning(f"Unknown record type: {record}")


def load_json_file(snow, filepath):
    """Load and insert records from a JSON file"""
    print(f"Loading data from {filepath}...")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        # Handle JSON array
        total_records = len(data)
        for i, record in enumerate(data):
            record_json = json.dumps(record)
            save_to_snowflake(snow, record_json)
            
            # Progress indicator
            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1}/{total_records} records...")
        
        print(f"✅ Completed loading {total_records} records from {filepath}")
    else:
        # Handle single JSON object
        record_json = json.dumps(data)
        save_to_snowflake(snow, record_json)
        print(f"✅ Loaded 1 record from {filepath}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python py_insert_arbore.py <json_file>")
        print("  python py_insert_arbore.py data_out/orders/orders.json")
        print("  python py_insert_arbore.py data_out/claims/warranty_claims.json")
        print("  echo 'json_record' | python py_insert_arbore.py --stdin")
        sys.exit(1)
    
    snow = connect_snow()
    
    try:
        if sys.argv[1] == '--stdin':
            # Read from stdin (pipe input)
            print("Reading from stdin...")
            for message in sys.stdin:
                if message.strip() and message.strip() != '\n':
                    save_to_snowflake(snow, message.strip())
                else:
                    break
            print("✅ Stdin input processing complete")
        else:
            # Read from JSON file
            filepath = sys.argv[1]
            if not os.path.exists(filepath):
                print(f"❌ Error: File {filepath} not found")
                sys.exit(1)
            
            # Only process JSON files
            if not filepath.lower().endswith('.json'):
                print(f"❌ Error: Only JSON files are supported. Got: {filepath}")
                sys.exit(1)
            
            load_json_file(snow, filepath)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Error during insertion: {e}")
    finally:
        snow.close()
        logging.info("Snowflake connection closed")