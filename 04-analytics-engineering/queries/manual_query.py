#!/usr/bin/env python3
"""
Manual Query Runner
Use this if DuckDB CLI is not installed
"""

import duckdb
import sys

def main():
    print("\n" + "="*60)
    print("DUCKDB MANUAL QUERY INTERFACE")
    print("="*60)
    print("\nConnecting to taxi_rides_ny.duckdb...\n")
    
    conn = duckdb.connect('/workspace/taxi_rides_ny/taxi_rides_ny.duckdb')
    
    # List available tables
    print("📊 Available tables:")
    print("-" * 60)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()
        print(f"  • {table[0]:35} {result[0]:>15,} records")
    
    print("\n" + "="*60)
    print("EXAMPLE QUERIES - Copy and modify as needed")
    print("="*60 + "\n")
    
    queries = {
        "Q3 - Count fct_monthly_zone_revenue": """
SELECT COUNT(*) FROM fct_monthly_zone_revenue;
        """,
        
        "Q4 - Best Green zone 2020": """
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green' AND pickup_year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
        """,
        
        "Q5 - Green trips Oct 2019": """
SELECT SUM(total_monthly_trips)
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND pickup_year = 2019 
  AND pickup_month = 10;
        """,
        
        "Q6 - FHV records": """
SELECT COUNT(*) FROM stg_fhv_tripdata;
        """,
        
        "Percentiles April 2020": """
SELECT 
    service_type,
    p90_fare,
    p95_fare,
    p97_fare,
    trip_count
FROM fct_monthly_fare_percentiles
WHERE year = 2020 AND month_number = 4
ORDER BY service_type;
        """,
        
        "Top 5 Growing Zones Jan 2020": """
SELECT 
    pickup_zone,
    service_type,
    mom_growth_pct
FROM fct_monthly_revenue_growth
WHERE year = 2020 
  AND month_number = 1 
  AND mom_growth_pct IS NOT NULL
ORDER BY mom_growth_pct DESC
LIMIT 5;
        """
    }
    
    for title, query in queries.items():
        print(f"\n{title}")
        print("-" * 60)
        print(f"Query:{query}")
        
        try:
            result = conn.execute(query.strip()).fetchall()
            print(f"Result:")
            for row in result:
                print(f"  {row}")
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\n" + "="*60)
    print("To run custom queries:")
    print("  python3 /workspace/queries/manual_query.py")
    print("="*60 + "\n")
    
    conn.close()

if __name__ == "__main__":
    main()
