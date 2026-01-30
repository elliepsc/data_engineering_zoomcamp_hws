#!/usr/bin/env python3
"""
Module 4 Homework - 2026 Version
Automated queries for all 6 homework questions
"""

import duckdb
import sys

def main():
    try:
        conn = duckdb.connect('/workspace/taxi_rides_ny/taxi_rides_ny.duckdb')
        
        print("\n" + "="*60)
        print("MODULE 4 HOMEWORK ANSWERS - 2026")
        print("="*60 + "\n")
        
        # Q1: Lineage (conceptual)
        print("Q1: dbt Lineage")
        print("─" * 60)
        print("Question: If you run 'dbt run --select int_trips_unioned',")
        print("          what models will be built?")
        print("")
        print("Answer: A - stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned")
        print("")
        print("Explanation: dbt builds upstream dependencies by default.")
        print("             The --select flag builds the specified model plus")
        print("             all models it depends on (refs).")
        print("")
        
        # Q2: Tests (conceptual)
        print("Q2: dbt Tests Behavior")
        print("─" * 60)
        print("Question: Test 'accepted_values: [1,2,3,4,5]' exists on")
        print("          payment_type. New value 6 appears. What happens?")
        print("")
        print("Answer: B - dbt will fail the test, returning a non-zero exit code")
        print("")
        print("Explanation: The accepted_values test fails when it encounters")
        print("             values not in the specified list.")
        print("")
        
        # Q3: Count fct_monthly_zone_revenue
        print("Q3: Count fct_monthly_zone_revenue")
        print("─" * 60)
        result = conn.execute("SELECT COUNT(*) FROM fct_monthly_zone_revenue").fetchone()
        print(f"Answer: {result[0]:,} records")
        print("")
        
        # Q4: Best Green zone 2020
        print("Q4: Best Performing Green Zone in 2020")
        print("─" * 60)
        result = conn.execute("""
            SELECT 
                pickup_zone,
                SUM(revenue_monthly_total_amount) as total_revenue
            FROM fct_monthly_zone_revenue
            WHERE service_type = 'Green' 
              AND pickup_year = 2020
            GROUP BY pickup_zone
            ORDER BY total_revenue DESC
            LIMIT 1
        """).fetchone()
        print(f"Answer: {result[0]}")
        print(f"Revenue: ${result[1]:,.2f}")
        print("")
        
        # Q5: Green trips Oct 2019
        print("Q5: Green Taxi Trips in October 2019")
        print("─" * 60)
        result = conn.execute("""
            SELECT SUM(total_monthly_trips)
            FROM fct_monthly_zone_revenue
            WHERE service_type = 'Green' 
              AND pickup_year = 2019 
              AND pickup_month = 10
        """).fetchone()
        print(f"Answer: {result[0]:,} trips")
        print("")
        
        # Q6: FHV count
        print("Q6: FHV Records After Filtering")
        print("─" * 60)
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM stg_fhv_tripdata
        """).fetchone()
        print(f"Answer: {result[0]:,} records")
        print("")
        print("Note: Filter applied: dispatching_base_num IS NOT NULL")
        print("")
        
        print("="*60)
        print("✅ All homework questions answered!")
        print("="*60)
        print("")
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())

# # bash:
# make homework-2026
# # or 
# # docker: 
# docker-compose exec dbt python3 /workspace/homework_queries_2026.py
# # sql file:
# docker-compose exec dbt duckdb taxi_rides_ny.duckdb < homework_2026.sql
# # interactive sql:
# docker-compose exec dbt bash
# duckdb taxi_rides_ny.duckdb
# # Copie-colle les queries depuis homework_2026.sql
# # Créer analyses/homework_q3.sql puis
# dbt compile --select homework_q3