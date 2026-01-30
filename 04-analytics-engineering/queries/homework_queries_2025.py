#!/usr/bin/env python3
"""
Module 4 Homework - 2025 Level (Advanced Analytics)
Bonus queries demonstrating advanced SQL and dbt skills
"""

import duckdb
import sys

def main():
    try:
        conn = duckdb.connect('/workspace/taxi_rides_ny/taxi_rides_ny.duckdb')
        
        print("\n" + "="*60)
        print("BONUS: ADVANCED ANALYTICS (2025 Level)")
        print("="*60 + "\n")
        
        # Percentiles
        print("Fare Percentiles Analysis - April 2020")
        print("─" * 60)
        result = conn.execute("""
            SELECT 
                service_type,
                p90_fare,
                p95_fare,
                p97_fare,
                trip_count
            FROM fct_monthly_fare_percentiles
            WHERE year = 2020 AND month_number = 4
            ORDER BY service_type
        """).fetchall()
        
        for row in result:
            print(f"{row[0]:7} | P90=${row[1]:6.2f} | P95=${row[2]:6.2f} | P97=${row[3]:6.2f} | Trips: {row[4]:,}")
        
        print("")
        print("Skills: PERCENTILE_CONT, window functions, outlier detection")
        print("")
        
        # Revenue Growth
        print("Top 5 Growing Zones - January 2020 (MoM %)")
        print("─" * 60)
        result = conn.execute("""
            SELECT 
                pickup_zone,
                service_type,
                revenue,
                prev_month_revenue,
                mom_growth_pct
            FROM fct_monthly_revenue_growth
            WHERE year = 2020 
              AND month_number = 1 
              AND mom_growth_pct IS NOT NULL
            ORDER BY mom_growth_pct DESC
            LIMIT 5
        """).fetchall()
        
        for i, row in enumerate(result, 1):
            print(f"{i}. {row[0]:35} ({row[1]:6}) +{row[4]:6.1f}%")
            print(f"   Jan: ${row[2]:>10,.0f} | Dec: ${row[3]:>10,.0f}")
        
        print("")
        print("Skills: LAG window functions, MoM calculations, growth metrics")
        print("")
        
        # Declining Zones
        print("Top 5 Declining Zones - January 2020 (MoM %)")
        print("─" * 60)
        result = conn.execute("""
            SELECT 
                pickup_zone,
                service_type,
                mom_growth_pct
            FROM fct_monthly_revenue_growth
            WHERE year = 2020 
              AND month_number = 1 
              AND mom_growth_pct IS NOT NULL
            ORDER BY mom_growth_pct ASC
            LIMIT 5
        """).fetchall()
        
        for i, row in enumerate(result, 1):
            print(f"{i}. {row[0]:35} ({row[1]:6}) {row[2]:6.1f}%")
        
        print("")
        
        # Summary Stats
        print("Summary: All Models")
        print("─" * 60)
        
        models = [
            ('fct_trips', 'Main fact table'),
            ('fct_monthly_zone_revenue', 'Monthly aggregates'),
            ('fct_monthly_fare_percentiles', 'Percentile analysis'),
            ('fct_monthly_revenue_growth', 'Growth tracking'),
        ]
        
        for model, description in models:
            result = conn.execute(f"SELECT COUNT(*) FROM {model}").fetchone()
            print(f"{model:30} {result[0]:>12,}  # {description}")
        
        print("")
        print("="*60)
        print("✅ Advanced analytics complete!")
        print("")
        print("Interview talking point:")
        print("'I extended the base curriculum with advanced SQL analytics")
        print(" including percentiles (P90/P95/P97), window functions (LAG),")
        print(" and reusable dbt macros for cross-warehouse compatibility.'")
        print("="*60)
        print("")
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
