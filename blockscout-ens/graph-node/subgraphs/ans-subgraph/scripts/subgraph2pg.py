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
  domains(first: $first, skip: $skip) {
    id
    name
    labelName
    labelhash
    parent { id }
    subdomains { id }
    resolvedAddress { id }
    resolver { id }
    ttl
    isMigrated
    createdAt
    owner { id }
  }
}
"""

def fetch_page(skip, first=100):
    """拉取单页数据"""
    variables = {"first": first, "skip": skip}
    response = requests.post(graphql_url, json={"query": query, "variables": variables})
    if response.status_code == 200:
        res_json = response.json()
        if "errors" in res_json:
            raise Exception(f"GraphQL 错误: {res_json['errors']}")
        return res_json["data"]["domains"]
    else:
        raise Exception(f"请求失败，状态码：{response.status_code}")

def fetch_all_domains_concurrent(first=100, max_workers=10, max_records=None):
    all_domains = []
    skip = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            if max_records and len(all_domains) >= max_records:
                break
                
            # 构造当前批次的任务列表
            futures = {
                executor.submit(fetch_page, skip + i * first, first): skip + i * first
                for i in range(max_workers)
            }
            skip += max_workers * first  # 预先累加跳过的记录数
            stop_fetch = False
            for future in concurrent.futures.as_completed(futures):
                page = future.result()
                if page:
                    all_domains.extend(page)
                    print(f"已拉取 {len(all_domains)} 条数据...")
                # 如果某一页数据不足 first，则说明后续没有更多数据
                if len(page) < first:
                    stop_fetch = True
                if max_records and len(all_domains) >= max_records:
                    stop_fetch = True
                    all_domains = all_domains[:max_records]  # 截取所需数量
                    break
            if stop_fetch:
                break
    return all_domains

def connect_pg():
    """建立 PostgreSQL 连接（请根据实际情况修改连接参数）"""
    conn = psycopg2.connect(
        host="localhost",         # PG 主机
        database="ddl-test",      # 数据库名称
        user="ddl",               # 用户名
        password="196526"         # 密码
    )
    return conn

def create_table(cur):
    """创建数据表（如果不存在）"""
    create_table_sql = """
    Create schema if not exists sgd1;
    CREATE TABLE IF NOT EXISTS sgd1.domain (
        vid BIGSERIAL PRIMARY KEY,
        block_range INT4RANGE NOT NULL,
        id TEXT NOT NULL,
        name TEXT,
        label_name TEXT,
        labelhash BYTEA,
        parent TEXT,
        subdomain_count INTEGER NOT NULL,
        resolved_address TEXT,
        resolver TEXT,
        ttl NUMERIC,
        is_migrated BOOLEAN NOT NULL,
        created_at NUMERIC NOT NULL,
        owner TEXT NOT NULL,
        registrant TEXT,
        wrapped_owner TEXT,
        expiry_date NUMERIC
        );
    """
    cur.execute(create_table_sql)


def bulk_insert_domains(cur, domains):
    """利用批量插入提高写入效率"""
    insert_sql = """
    INSERT INTO sgd1.domain (
        block_range,
        id,
        name,
        label_name,
        labelhash,
        parent,
        subdomain_count,
        resolved_address,
        resolver,
        ttl,
        is_migrated,
        created_at,
        owner,
        registrant,
        wrapped_owner,
        expiry_date
    ) VALUES %s
    ON CONFLICT (vid) DO UPDATE SET 
        block_range = EXCLUDED.block_range,
        id = EXCLUDED.id,
        name = EXCLUDED.name,
        label_name = EXCLUDED.label_name,
        labelhash = EXCLUDED.labelhash,
        parent = EXCLUDED.parent,
        subdomain_count = EXCLUDED.subdomain_count,
        resolved_address = EXCLUDED.resolved_address,
        resolver = EXCLUDED.resolver,
        ttl = EXCLUDED.ttl,
        is_migrated = EXCLUDED.is_migrated,
        created_at = EXCLUDED.created_at,
        owner = EXCLUDED.owner,
        registrant = EXCLUDED.registrant,
        wrapped_owner = EXCLUDED.wrapped_owner,
        expiry_date = EXCLUDED.expiry_date;
    """
    values = []
    for domain in domains:
        values.append((
            '[0,)' if domain.get("id") else '[0,1)',  # block_range default
            domain.get("id"),
            domain.get("name"),
            domain.get("labelName"),  # label_name
            domain.get("labelhash"),  # labelhash
            None if domain.get("parent") is None else domain.get("parent", {}).get("id"),
            len(domain.get("subdomains", [])),
            None if domain.get("resolvedAddress") is None else domain.get("resolvedAddress", {}).get("id"),
            None if domain.get("resolver") is None else domain.get("resolver", {}).get("id"),
            domain.get("ttl"),
            domain.get("isMigrated", False),
            domain.get("createdAt", "0"),
            None if domain.get("owner") is None else domain.get("owner", {}).get("id", ""),  # owner is NOT NULL
            None,
            None,
            None
        ))
    execute_values(cur, insert_sql, values, page_size=100)

def main():
    parser = argparse.ArgumentParser(description='从 ANS Subgraph 拉取数据并存入 PostgreSQL')
    parser.add_argument('--batch-size', type=int, default=100, help='每批次获取的数据量 (默认: 100)')
    parser.add_argument('--max-records', type=int, help='最大获取记录数')
    parser.add_argument('--workers', type=int, default=5, help='并发线程数 (默认: 10)')
    args = parser.parse_args()

    try:
        # 并发拉取数据
        domains = fetch_all_domains_concurrent(
            first=args.batch_size,
            max_workers=args.workers,
            max_records=args.max_records
        )
        print(f"共获取到 {len(domains)} 条数据。")
        
        # 连接 PostgreSQL 并创建表
        conn = connect_pg()
        cur = conn.cursor()
        create_table(cur)
        conn.commit()
        
        # 批量插入数据
        bulk_insert_domains(cur, domains)
        conn.commit()
        print("数据批量写入 PG 成功！")
        
    except Exception as e:
        print("发生错误：", e)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
