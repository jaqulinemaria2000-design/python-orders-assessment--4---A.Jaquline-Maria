import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def create_source_db(db_path, data_dir):
    """Creates a mock SQLite source database with required tables and data anomalies."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Create customer_profile table
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
            updated_at TIMESTAMP
        )
    """)
    
    # Generate customer_profile data
    # We will read customers.csv to get some real customer IDs if available, 
    # but we can also just generate mock ones that overlap.
    customers_file = os.path.join(data_dir, 'customers.csv')
    if os.path.exists(customers_file):
        customers_df = pd.read_csv(customers_file)
        cust_ids = customers_df['customer_id'].tolist()[:50] # Take first 50
    else:
        cust_ids = [f"C{str(i).zfill(4)}" for i in range(1, 51)]

    profiles = []
    now = datetime.now()
    
    for cid in cust_ids:
        # Generate 1 to 3 SCD2 records per customer
        num_records = np.random.randint(1, 4)
        
        current_date = now - timedelta(days=np.random.randint(100, 365))
        
        for i in range(num_records):
            is_last = (i == num_records - 1)
            
            # Anomaly: Overlapping effective ranges (10% chance)
            overlap_days = np.random.randint(1, 15) if np.random.random() < 0.1 and not is_last else 0
            
            effective_from = current_date
            if is_last:
                effective_to = None
                is_current = True
            else:
                effective_to = effective_from + timedelta(days=np.random.randint(30, 90))
                is_current = False
            
            # Anomaly: Inconsistent country (10% chance)
            # In simple terms, just use a random country
            countries = ['US', 'IN', 'GB', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR']
            country = np.random.choice(countries)
            
            segments = ['Gold', 'Silver', 'Bronze', 'SMB', 'Enterprise']
            kyc = ['VERIFIED', 'PENDING', 'REJECTED']
            
            profiles.append({
                'customer_id': cid,
                'segment': np.random.choice(segments),
                'kyc_status': np.random.choice(kyc),
                'risk_score': np.random.randint(0, 101),
                'country_iso2': country,
                'effective_from': effective_from.strftime('%Y-%m-%d %H:%M:%S'),
                'effective_to': effective_to.strftime('%Y-%m-%d %H:%M:%S') if effective_to else None,
                'is_current': is_current,
                'updated_at': now.strftime('%Y-%m-%d %H:%M:%S')
            })
            
            if not is_last:
                current_date = effective_to - timedelta(days=overlap_days)

    pd.DataFrame(profiles).to_sql('customer_profile', conn, if_exists='replace', index=False)

    # 2. Create fx_rates_daily table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates_daily (
            rate_date DATE,
            base_currency TEXT,
            quote_currency TEXT,
            fx_rate NUMERIC,
            source TEXT,
            ingested_at TIMESTAMP
        )
    """)
    
    # Generate fx_rates data
    # Let's generate dates for the last year
    start_date = now.date() - timedelta(days=365)
    dates = [start_date + timedelta(days=i) for i in range(365)]
    
    currencies = ['EUR', 'INR', 'GBP']
    fx_data = []
    
    for date in dates:
        for cur in currencies:
            # Base rate
            if cur == 'EUR': rate = 0.92
            elif cur == 'INR': rate = 83.0
            elif cur == 'GBP': rate = 0.79
            
            # Add some noise
            rate = rate * (1 + np.random.normal(0, 0.01))
            
            # Anomaly 1: Missing rates (5% chance)
            if np.random.random() < 0.05:
                continue
                
            # Anomaly 3: Outlier spike (2% chance)
            if np.random.random() < 0.02:
                rate = rate * np.random.choice([0.1, 10.0])
                
            fx_data.append({
                'rate_date': date.strftime('%Y-%m-%d'),
                'base_currency': 'USD',
                'quote_currency': cur,
                'fx_rate': round(rate, 4),
                'source': 'API_PRIMARY',
                'ingested_at': now.strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Anomaly 2: Duplicate rates from different sources (10% chance)
            if np.random.random() < 0.10:
                # Slightly different rate
                alt_rate = rate * (1 + np.random.normal(0, 0.005))
                fx_data.append({
                    'rate_date': date.strftime('%Y-%m-%d'),
                    'base_currency': 'USD',
                    'quote_currency': cur,
                    'fx_rate': round(alt_rate, 4),
                    'source': 'API_SECONDARY',
                    'ingested_at': now.strftime('%Y-%m-%d %H:%M:%S')
                })
                
    pd.DataFrame(fx_data).to_sql('fx_rates_daily', conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()
    
    print(f"Source SQLite database created successfully at '{db_path}'.")
    print(f"   Generated {len(profiles)} customer profiles (with SCD2 anamolies).")
    print(f"   Generated {len(fx_data)} FX rates (with missing/duplicate/outlier anamolies).")

if __name__ == "__main__":
    db_path = r"c:\Users\Jaquline Maria\Desktop\week 4\assignment\data\source.db"
    data_dir = r"c:\Users\Jaquline Maria\Desktop\week 4\data"
    create_source_db(db_path, data_dir)
