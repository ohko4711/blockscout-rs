import requests
import psycopg2
import json
import concurrent.futures
import datetime
import argparse
from psycopg2.extras import execute_values

graphql_url = "https://subgraph.acedomains.io/subgraphs/name/acedomains/ans"
query = """
query MyQuery($first: Int!, $skip: Int!) {
  registrations(first: $first, skip: $skip) {
    id
    domain {
      id
    }
    registrationDate
    expiryDate
    cost
    registrant {
      id
    }
    labelName
  }
}
"""

def fetch_page(skip, first=100):
    """Fetch a single page of data"""
    variables = {"first": first, "skip": skip}
    response = requests.post(graphql_url, json={"query": query, "variables": variables})
    if response.status_code == 200:
        res_json = response.json()
        if "errors" in res_json:
            raise Exception(f"GraphQL error: {res_json['errors']}")
        return res_json["data"]["registrations"]
    else:
        raise Exception(f"Request failed with status code: {response.status_code}")

def fetch_all_registrations_concurrent(first=100, max_workers=10, max_records=None):
    all_registrations = []
    skip = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            if max_records and len(all_registrations) >= max_records:
                break
                
            # Create tasks for current batch
            futures = {
                executor.submit(fetch_page, skip + i * first, first): skip + i * first
                for i in range(max_workers)
            }
            skip += max_workers * first  # Pre-increment skipped records
            stop_fetch = False
            for future in concurrent.futures.as_completed(futures):
                page = future.result()
                if page:
                    all_registrations.extend(page)
                    print(f"Fetched {len(all_registrations)} records...")
                # If a page has less than first items, no more data exists
                if len(page) < first:
                    stop_fetch = True
                if max_records and len(all_registrations) >= max_records:
                    stop_fetch = True
                    all_registrations = all_registrations[:max_records]  # Truncate to requested size
                    break
            if stop_fetch:
                break
    return all_registrations

def connect_pg():
    """Establish PostgreSQL connection"""
    conn = psycopg2.connect(
        host="localhost",
        database="ddl-test",
        user="ddl",
        password="196526"
    )
    return conn

def create_table(cur):
    """Create the registration table if it doesn't exist"""
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS sgd1;
    CREATE TABLE IF NOT EXISTS sgd1.registration (
        vid BIGSERIAL PRIMARY KEY,
        block_range INT4RANGE NOT NULL,
        id TEXT NOT NULL,
        domain TEXT NOT NULL,
        registration_date NUMERIC NOT NULL,
        expiry_date NUMERIC NOT NULL,
        cost NUMERIC,
        registrant TEXT NOT NULL,
        label_name TEXT
    );
    """
    cur.execute(create_table_sql)

def bulk_insert_registrations(cur, registrations):
    """Bulk insert registrations data"""
    insert_sql = """
    INSERT INTO sgd1.registration (
        block_range,
        id,
        domain,
        registration_date,
        expiry_date,
        cost,
        registrant,
        label_name
    ) VALUES %s
    ON CONFLICT (vid) DO UPDATE SET 
        block_range = EXCLUDED.block_range,
        id = EXCLUDED.id,
        domain = EXCLUDED.domain,
        registration_date = EXCLUDED.registration_date,
        expiry_date = EXCLUDED.expiry_date,
        cost = EXCLUDED.cost,
        registrant = EXCLUDED.registrant,
        label_name = EXCLUDED.label_name;
    """
    values = []
    for reg in registrations:
        values.append((
            '[0,)' if reg.get("id") else '[0,1)',  # block_range default
            reg.get("id"),
            None if reg.get("domain") is None else reg.get("domain", {}).get("id"),
            reg.get("registrationDate", "0"),
            reg.get("expiryDate", "0"),
            reg.get("cost"),
            None if reg.get("registrant") is None else reg.get("registrant", {}).get("id"),
            reg.get("labelName")
        ))
    execute_values(cur, insert_sql, values, page_size=100)

def main():
    parser = argparse.ArgumentParser(description='Fetch data from ANS Subgraph and store in PostgreSQL')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for each fetch (default: 100)')
    parser.add_argument('--max-records', type=int, help='Maximum number of records to fetch')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers (default: 5)')
    args = parser.parse_args()

    try:
        # Fetch data concurrently
        registrations = fetch_all_registrations_concurrent(
            first=args.batch_size,
            max_workers=args.workers,
            max_records=args.max_records
        )
        print(f"Total records fetched: {len(registrations)}")
        
        # Connect to PostgreSQL and create table
        conn = connect_pg()
        cur = conn.cursor()
        create_table(cur)
        conn.commit()
        
        # Bulk insert data
        bulk_insert_registrations(cur, registrations)
        conn.commit()
        print("Data successfully written to PostgreSQL!")
        
    except Exception as e:
        print("Error occurred:", e)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
