# Arboré Snowpipe Data Pipeline

This project implements a complete data pipeline for the Arboré watch business, using Snowflake's Snowpipe for serverless data ingestion from generated JSON files to Snowflake tables.

## 🏗️ Architecture Overview

```
Data Generator → JSON Files → Snowpipe → Snowflake Tables
```

- **Data Generator**: Creates realistic dirty data (orders, warranty claims, suppliers)
- **Snowpipe**: Serverless ingestion service that automatically loads data into Snowflake
- **Snowflake Tables**: `ARBORE_ORDERS` and `ARBORE_WARRANTY_CLAIMS` in the `INGEST` database

## 📁 Project Structure

```
Arbore/
├── FINAL_data_generator.py          # Generates sample data
├── py_snowpipe_arbore.py           # Main Snowpipe processing script
├── arbore_snowpipe_setup.sql       # SQL setup for Snowpipe infrastructure
├── check_snowpipe_status.py        # Verification script for data counts
├── .env                            # Environment variables (not in repo)
└── data_out/                       # Generated data files
    ├── orders/
    │   └── orders.json
    ├── claims/
    │   └── warranty_claims.json
    └── suppliers/
        └── suppliers.json
```

## 🔧 Prerequisites

1. **Python Environment**: 
   ```bash
   pip install snowflake-connector-python snowflake-ingest pandas pyarrow python-dotenv
   ```

2. **Snowflake Setup**: 
   - Execute `arbore_snowpipe_setup.sql` in your Snowflake environment
   - Configure RSA key pair authentication

3. **Environment Variables** (`.env` file):
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PRIVATE_KEY_PATH=path_to_private_key.p8
   SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase
   ```

## 🚀 How to Use the Pipeline

### Step 1: Generate Data

Configure the data volume in `FINAL_data_generator.py`:
```python
NUM_ORDERS = 1000      # Number of orders to generate
NUM_CLAIMS = 30        # Number of warranty claims to generate
```

Generate the data:
```bash
python FINAL_data_generator.py
```

**Output**: Creates JSON files in `data_out/` directory with realistic business data including intentional data quality issues.

### Step 2: Load Data via Snowpipe

#### Load Orders:
```bash
python py_snowpipe_arbore.py data_out/orders/orders.json 500
```
- `data_out/orders/orders.json`: Path to the orders JSON file
- `500`: Batch size (recommended for optimal performance)

#### Load Warranty Claims:
```bash
python py_snowpipe_arbore.py data_out/claims/warranty_claims.json 30
```
- `data_out/claims/warranty_claims.json`: Path to the claims JSON file  
- `30`: Batch size (adjust based on data volume)

### Step 3: Verify Data Load

Check the data counts in Snowflake:
```bash
python check_snowpipe_status.py
```

## 📊 Snowpipe Processing Details

### How It Works

1. **Auto-Detection**: The script automatically detects whether you're loading orders or claims based on the file path
2. **Batch Processing**: Data is processed in configurable batches for optimal performance
3. **Parquet Conversion**: JSON data is converted to Parquet format with SNAPPY compression
4. **File Upload**: Files are uploaded to Snowflake internal stage
5. **Snowpipe Trigger**: The appropriate Snowpipe is triggered for serverless ingestion

### Supported Data Types

- **Orders**: Customer orders with order details, dates, amounts
- **Warranty Claims**: Product warranty claims with claim details and status

### Performance Recommendations

- **Batch Sizes**: 
  - Orders: 500-2000 records per batch
  - Claims: 30-100 records per batch
- **File Format**: Parquet with SNAPPY compression (automatically handled)
- **Large Datasets**: The pipeline successfully processes 100,000+ records

## 🔍 Example Usage Scenarios

### Small Test Load (1,000 orders + 30 claims):
```bash
# 1. Configure in FINAL_data_generator.py: NUM_ORDERS=1000, NUM_CLAIMS=30
# 2. Generate data
python FINAL_data_generator.py

# 3. Load orders
python py_snowpipe_arbore.py data_out/orders/orders.json 500

# 4. Load claims  
python py_snowpipe_arbore.py data_out/claims/warranty_claims.json 30

# 5. Verify
python check_snowpipe_status.py
```

### Large Scale Load (100,000 orders + 3,000 claims):
```bash
# 1. Configure in FINAL_data_generator.py: NUM_ORDERS=100000, NUM_CLAIMS=3000
# 2. Generate data
python FINAL_data_generator.py

# 3. Load orders (larger batch size for efficiency)
python py_snowpipe_arbore.py data_out/orders/orders.json 2000

# 4. Load claims
python py_snowpipe_arbore.py data_out/claims/warranty_claims.json 100

# 5. Verify
python check_snowpipe_status.py
```

## 🛠️ Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify `.env` file configuration and RSA key setup
2. **File Path Issues**: Ensure paths use forward slashes or let the script handle Windows path conversion
3. **Batch Size**: Adjust batch sizes if you encounter memory issues with large datasets

### File Locations

- Generated data: `data_out/orders/orders.json`, `data_out/claims/warranty_claims.json`
- Snowflake stage: Files are temporarily stored in Snowflake internal stages
- Target tables: `INGEST.PUBLIC.ARBORE_ORDERS`, `INGEST.PUBLIC.ARBORE_WARRANTY_CLAIMS`

## 📈 Pipeline Benefits

- **Serverless**: No infrastructure management required
- **Scalable**: Handles datasets from thousands to millions of records
- **Automated**: Minimal manual intervention once configured
- **Reliable**: Built-in error handling and retry mechanisms
- **Cost-Effective**: Pay only for actual data processing

## 🔗 Key Components

- **Snowpipe**: `ARBORE_ORDERS_PIPE` and `ARBORE_WARRANTY_CLAIMS_PIPE`
- **File Formats**: `FF_JSON_ORDERS` and `FF_JSON_CLAIMS`
- **Stages**: Internal Snowflake stages for temporary file storage
- **Authentication**: RSA key pair for secure, automated access

---

**Note**: This pipeline is designed for the Arboré watch business use case but can be adapted for other business scenarios by modifying the data generation logic and table schemas.