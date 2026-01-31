#!/usr/bin/env python3
"""
Module 4 Homework - 2026 Version (with Tabulate)
Beautiful table formatting for homework queries
"""

import duckdb
import sys

try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False
    print("⚠️  Warning: tabulate not installed. Using plain output.")
    print("   Install with: pip install tabulate")
    print()

def print_table(data, headers, title=None):
    """Print data as formatted table or plain text"""
    if title:
        print(f"\n{title}")
        print("─" * 60)
    
    if TABULATE_AVAILABLE:
        print(tabulate(data, headers=headers, tablefmt='grid', floatfmt=".2f"))
    else:
        # Fallback to plain text
        print(" | ".join(headers))
        print("-" * 60)
        for row in data:
            print(" | ".join(str(x) for x in row))

def main():
    try:
        conn = duckdb.connect('/workspace/taxi_rides_ny/taxi_rides_ny.duckdb')
        
        print("\n" + "="*70)
        print("MODULE 4 HOMEWORK ANSWERS - 2026 (WITH TABULATE)")
        print("="*70 + "\n")
        
        # Q1: Lineage (conceptual)
        print("Q1: dbt Lineage")
        print("─" * 70)
        print("Question: If you run 'dbt run --select int_trips_unioned',")
        print("          what models will be built?")
        print("")
        
        lineage_data = [
            ["A", "stg_green_tripdata, stg_yellow_tripdata, int_trips_unioned", "✅ Correct"],
            ["B", "int_trips_unioned only", "❌"],
            ["C", "All models", "❌"],
            ["D", "No models", "❌"]
        ]
        print_table(
            lineage_data,
            headers=["Option", "Models Built", ""],
            title=None
        )
        
        print("\nAnswer: A")
        print("Explanation: dbt builds upstream dependencies by default.")
        print("")
        
        # Q2: Tests (conceptual)
        print("\nQ2: dbt Tests Behavior")
        print("─" * 70)
        print("Question: Test 'accepted_values: [1,2,3,4,5]' on payment_type.")
        print("          New value 6 appears. What happens?")
        print("")
        
        test_data = [
            ["A", "dbt ignores the new value", "❌"],
            ["B", "dbt fails the test (non-zero exit)", "✅ Correct"],
            ["C", "dbt logs a warning", "❌"],
            ["D", "dbt auto-updates the test", "❌"]
        ]
        print_table(
            test_data,
            headers=["Option", "Behavior", ""],
            title=None
        )
        
        print("\nAnswer: B")
        print("Explanation: accepted_values test fails on unexpected values.")
        print("")
        
        # Q3: Count fct_monthly_zone_revenue
        result = conn.execute("SELECT COUNT(*) as count FROM fct_monthly_zone_revenue").fetchall()
        print_table(
            result,
            headers=["Total Records"],
            title="\nQ3: Count fct_monthly_zone_revenue"
        )
        
        # Q4: Best Green zone 2020
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
        """).fetchall()
        print_table(
            result,
            headers=["Pickup Zone", "Total Revenue 2020"],
            title="\nQ4: Best Performing Green Zone in 2020"
        )
        
        # Q5: Green trips Oct 2019
        result = conn.execute("""
            SELECT SUM(total_monthly_trips) as total_trips
            FROM fct_monthly_zone_revenue
            WHERE service_type = 'Green' 
              AND pickup_year = 2019 
              AND pickup_month = 10
        """).fetchall()
        print_table(
            result,
            headers=["Total Green Trips"],
            title="\nQ5: Green Taxi Trips in October 2019"
        )
        
        # Q6: FHV records
        result = conn.execute("""
            SELECT COUNT(*) as fhv_records
            FROM stg_fhv_tripdata
        """).fetchall()
        print_table(
            result,
            headers=["FHV Records (after filter)"],
            title="\nQ6: FHV Records After Filtering"
        )
        print("Note: Filter applied: dispatching_base_num IS NOT NULL")
        
        # Summary table
        print("\n" + "="*70)
        print("SUMMARY: All Answers")
        print("="*70)
        
        summary = conn.execute("""
            WITH answers AS (
                SELECT 'Q3' as question, 'fct_monthly_zone_revenue count' as description, 
                       CAST(COUNT(*) AS VARCHAR) as answer
                FROM fct_monthly_zone_revenue
                UNION ALL
                SELECT 'Q4', 'Best Green zone 2020', pickup_zone
                FROM (
                    SELECT pickup_zone, SUM(revenue_monthly_total_amount) as rev
                    FROM fct_monthly_zone_revenue
                    WHERE service_type = 'Green' AND pickup_year = 2020
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 1
                )
                UNION ALL
                SELECT 'Q5', 'Green trips Oct 2019', CAST(SUM(total_monthly_trips) AS VARCHAR)
                FROM fct_monthly_zone_revenue
                WHERE service_type = 'Green' AND pickup_year = 2019 AND pickup_month = 10
                UNION ALL
                SELECT 'Q6', 'FHV records (filtered)', CAST(COUNT(*) AS VARCHAR)
                FROM stg_fhv_tripdata
            )
            SELECT * FROM answers
        """).fetchall()
        
        print_table(
            summary,
            headers=["Question", "Description", "Answer"],
            title=None
        )
        
        print("\n" + "="*70)
        print("✅ All homework questions answered!")
        print("="*70)
        print("")
        
        if not TABULATE_AVAILABLE:
            print("💡 Tip: Install tabulate for better formatting:")
            print("   Add 'tabulate==0.9.0' to requirements.txt")
            print("   Then: make build")
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
