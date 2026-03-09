import pandas as pd
import sqlite3
import os
import json

def ingest_data(data_dir, output_dir, source_db_path):
    """
    Ingests data from raw files and source SQLite DB, 
    and saves them as raw CSV files in an output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Ingest customers.csv
    customers_path = os.path.join(data_dir, 'customers.csv')
    if os.path.exists(customers_path):
        customers_df = pd.read_csv(customers_path)
        customers_df.to_csv(os.path.join(output_dir, 'raw_customers.csv'), index=False)
        print(f"Ingested {len(customers_df)} customers from CSV.")
    
    # 2. Ingest orders.json.json (or orders.json)
    orders_path = os.path.join(data_dir, 'orders.json.json')
    if not os.path.exists(orders_path):
        orders_path = os.path.join(data_dir, 'orders.json')
        
    if os.path.exists(orders_path):
        with open(orders_path, 'r') as f:
            # Assuming JSON might be line-delimited or a JSON array
            try:
                orders_df = pd.read_json(orders_path)
            except ValueError:
                orders_df = pd.read_json(orders_path, lines=True)
        orders_df.to_csv(os.path.join(output_dir, 'raw_orders.csv'), index=False)
        print(f"Ingested {len(orders_df)} orders from JSON.")
        
    # 3. Ingest payments.xlsx
    payments_path = os.path.join(data_dir, 'payments.xlsx')
    if os.path.exists(payments_path):
        payments_df = pd.read_excel(payments_path)
        payments_df.to_csv(os.path.join(output_dir, 'raw_payments.csv'), index=False)
        print(f"Ingested {len(payments_df)} payments from Excel.")
        
    # 4. Ingest from source SQLite 
    if os.path.exists(source_db_path):
        conn = sqlite3.connect(source_db_path)
        
        # customer_profile
        profile_df = pd.read_sql("SELECT * FROM customer_profile", conn)
        profile_df.to_csv(os.path.join(output_dir, 'raw_customer_profile.csv'), index=False)
        
        # fx_rates_daily
        fx_df = pd.read_sql("SELECT * FROM fx_rates_daily", conn)
        fx_df.to_csv(os.path.join(output_dir, 'raw_fx_rates_daily.csv'), index=False)
        
        conn.close()
        print(f"Ingested {len(profile_df)} customer profiles and {len(fx_df)} FX rates from source DB.")

if __name__ == "__main__":
    base_dir = r"c:\Users\Jaquline Maria\Desktop\week 4"
    ingest_data(
        data_dir=os.path.join(base_dir, 'data'),
        output_dir=os.path.join(base_dir, 'assignment', 'data', 'raw'),
        source_db_path=os.path.join(base_dir, 'assignment', 'data', 'source.db')
    )
