import sqlite3
import pandas as pd
import os

def create_oltp_schema(conn):
    """Creates the normalized OLTP schema with primary and foreign keys."""
    cursor = conn.cursor()
    
    # customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            country TEXT,
            signup_date TIMESTAMP
        )
    """)
    
    # customer_profile table (SCD2)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_profile (
            profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT NOT NULL,
            segment TEXT,
            kyc_status TEXT,
            risk_score INTEGER,
            country_iso2 TEXT,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            updated_at TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TIMESTAMP,
            amount NUMERIC,
            currency TEXT,
            status TEXT,
            items TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # payments table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            payment_date TIMESTAMP,
            payment_method TEXT,
            paid_amount NUMERIC,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
    """)
    
    # fx_rates_daily table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates_daily (
            rate_date DATE,
            base_currency TEXT,
            quote_currency TEXT,
            fx_rate NUMERIC,
            source TEXT,
            ingested_at TIMESTAMP,
            PRIMARY KEY (rate_date, base_currency, quote_currency)
        )
    """)
    
    conn.commit()

def load_oltp(cleaned_dir, oltp_db_path):
    """Loads cleaned intermediate files into the OLTP database."""
    os.makedirs(os.path.dirname(oltp_db_path), exist_ok=True)
    
    # Drop previous DB to allow re-runs of the DAG
    if os.path.exists(oltp_db_path):
        os.remove(oltp_db_path)
        
    conn = sqlite3.connect(oltp_db_path)
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = 1")
    
    # Create schema
    create_oltp_schema(conn)
    
    # Load Customers
    cust_path = os.path.join(cleaned_dir, 'cleaned_customers.csv')
    valid_customers = []
    if os.path.exists(cust_path):
        df = pd.read_csv(cust_path)
        valid_customers = df['customer_id'].unique()
        df.to_sql('customers', conn, if_exists='append', index=False)
        print(f"Loaded {len(df)} customers to OLTP.")

    # Load Customer Profiles
    prof_path = os.path.join(cleaned_dir, 'cleaned_customer_profile.csv')
    if os.path.exists(prof_path):
        df = pd.read_csv(prof_path)
        # Assuming missing column 'name' is in customer_profile? No.
        if 'profile_id' in df.columns:
            df.drop(columns=['profile_id'], inplace=True) # Let DB auto-increment
        df.to_sql('customer_profile', conn, if_exists='append', index=False)
        print(f"Loaded {len(df)} customer profiles to OLTP.")

    # Load Orders
    ord_path = os.path.join(cleaned_dir, 'cleaned_orders.csv')
    valid_orders = []
    if os.path.exists(ord_path):
        df = pd.read_csv(ord_path)
        df.drop_duplicates(subset=['order_id'], inplace=True)
        df = df[df['customer_id'].isin(valid_customers)]
        valid_orders = df['order_id'].unique()
        if 'items' in df.columns:
            df['items'] = df['items'].astype(str)
        df.to_sql('orders', conn, if_exists='append', index=False)
        print(f"Loaded {len(df)} orders to OLTP.")

    # Load Payments
    pay_path = os.path.join(cleaned_dir, 'cleaned_payments.csv')
    if os.path.exists(pay_path):
        df = pd.read_csv(pay_path)
        df.drop_duplicates(subset=['payment_id'], inplace=True) if 'payment_id' in df.columns else None
        df = df[df['order_id'].isin(valid_orders)]
        # there is no payment_id but order_id and payment_date is unique usually? the schema says payment_id integer primary key autoincrement
        # since it's autoincrement, it doesn't matter if we don't supply it. It shouldn't violate PK unless we pass it.
        # But wait, what if we provide payment_id? raw_payments didn't have payment_id.
        pass
        df.to_sql('payments', conn, if_exists='append', index=False)
        print(f"Loaded {len(df)} payments to OLTP.")

    # Load FX Rates
    fx_path = os.path.join(cleaned_dir, 'cleaned_fx_rates_daily.csv')
    if os.path.exists(fx_path):
        df = pd.read_csv(fx_path)
        df.to_sql('fx_rates_daily', conn, if_exists='append', index=False)
        print(f"Loaded {len(df)} FX rates to OLTP.")

    conn.close()

if __name__ == "__main__":
    base_dir = r"c:\Users\Jaquline Maria\Desktop\week 4\assignment"
    load_oltp(
        cleaned_dir=os.path.join(base_dir, 'data', 'cleaned'),
        oltp_db_path=os.path.join(base_dir, 'data', 'oltp.db')
    )
