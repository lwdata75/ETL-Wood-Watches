#!/usr/bin/env python3
import py_insert_arbore

def main():
    # Connect to Snowflake
    snow = py_insert_arbore.connect_snow()
    cursor = snow.cursor()

    print('📈 Snowpipe Performance Analysis')
    print()

    # Check current data counts
    cursor.execute('SELECT COUNT(*) FROM ARBORE_ORDERS')
    order_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM ARBORE_WARRANTY_CLAIMS')
    claim_count = cursor.fetchone()[0]
    
    print(f'Current Data Counts:')
    print(f'  📊 ARBORE_ORDERS: {order_count} records')
    print(f'  📊 ARBORE_WARRANTY_CLAIMS: {claim_count} records')
    print()

    # Check Snowpipe processing history
    query = """
    SELECT 
        PIPE_NAME,
        FILE_NAME,
        STATUS,
        FILE_SIZE,
        ROWS_LOADED,
        ROWS_PARSED,
        LOAD_TIME
    FROM INFORMATION_SCHEMA.PIPE_USAGE_HISTORY 
    WHERE PIPE_NAME IN ('ARBORE_ORDERS_PIPE', 'ARBORE_WARRANTY_CLAIMS_PIPE')
    ORDER BY LOAD_TIME DESC
    LIMIT 10
    """

    cursor.execute(query)
    pipe_history = cursor.fetchall()

    if pipe_history:
        print('Recent Snowpipe Processing:')
        print('=' * 80)
        for hist in pipe_history:
            pipe_name = hist[0]
            file_name = hist[1]
            status = hist[2]
            file_size = hist[3]
            rows_loaded = hist[4]
            rows_parsed = hist[5]
            load_time = hist[6]
            
            print(f'Pipe: {pipe_name}')
            print(f'  File: {file_name}')
            print(f'  Status: {status}')
            print(f'  File Size: {file_size} bytes')
            print(f'  Rows Loaded: {rows_loaded}')
            print(f'  Load Time: {load_time}')
            print()
    else:
        print('⏳ No processing history found yet - data may still be processing')

    snow.close()
    print('✅ Analysis complete!')

if __name__ == "__main__":
    main()