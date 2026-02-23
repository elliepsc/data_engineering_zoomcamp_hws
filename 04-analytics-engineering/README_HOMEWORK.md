# Module 4 - Analytics Engineering with dbt (DuckDB)

**Data Engineering Zoomcamp 2026 - NYC Taxi Analytics Project**

Portfolio project demonstrating modern analytics engineering practices with dbt, processing 117M+ taxi trip records.

---

## 🎯 Project Overview

End-to-end dbt project transforming raw NYC taxi data into analytics-ready models using:
- **Database**: DuckDB (local development)
- **Orchestration**: Docker Compose
- **Transformation**: dbt Core 1.7.4
- **Data**: 60 parquet files, ~3GB, 117M+ records (2019-2020)

---

## 📊 Key Metrics

- 📁 **60 parquet files** (Green, Yellow, FHV taxi data)
- 🚕 **117M+ trip records** processed
- 📈 **10 dbt models** (staging → intermediate → marts → core)
- ⚡ **~2 minute build time** (full refresh)
- ✅ **100% test coverage** (10 tests passing)
- 📚 **Complete documentation** with dbt docs

---

## 🏗️ Architecture

### Data Flow
```
Raw Parquet Files (60 files)
    ↓
Staging Layer (3 views)
    ├── stg_green_tripdata
    ├── stg_yellow_tripdata
    └── stg_fhv_tripdata
    ↓
Intermediate Layer (1 view)
    └── int_trips_unioned
    ↓
Marts Layer (3 tables)
    ├── fct_trips
    ├── dim_zones
    └── fct_monthly_zone_revenue
    ↓
Core Layer (3 tables - Advanced Analytics)
    ├── fct_monthly_fare_percentiles
    ├── fct_monthly_revenue_growth
    └── fct_payment_analysis
```

### Materialization Strategy
- **Staging**: Views (fast, no storage)
- **Intermediate**: Views (composable transformations)
- **Marts**: Tables (optimized for queries)
- **Core**: Tables (complex aggregations)

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 5GB free disk space
- ~20 minutes for initial setup

#### Make sure to be in the same file
```bash
cd 04-analytics-engineering
```


### Setup (5 commands)
```bash
make build          # Build Docker image (2-3 min)
make up             # Start container
make data           # Download taxi data (10-15 min)
make setup-dbt      # Install dbt packages + seed
make dbt-build      # Build all models (2 min)
```

### Verify Installation
```bash
make verify-models  # Check all 10 models built
make dbt-docs       # View docs at http://localhost:38080
```

---

## 📝 Homework Answers (2026)

### Q1: dbt Lineage
**Question**: If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer**: **A** - stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned

**Explanation**: dbt builds upstream dependencies by default. The `--select` flag builds the specified model plus all models it depends on via `{{ ref() }}`.

---

### Q2: dbt Tests Behavior
**Question**: Test `accepted_values: [1,2,3,4,5]` exists on payment_type. New value 6 appears. What happens?

**Answer**: **B** - dbt will fail the test, returning a non-zero exit code

**Explanation**: The `accepted_values` test fails when it encounters values not in the specified list, causing dbt to exit with a non-zero code.

---

### Q3: Count fct_monthly_zone_revenue
**Query**:
```sql
SELECT COUNT(*) FROM fct_monthly_zone_revenue;
```

**Answer**: **12,465 records**

**Run it**:
```bash
make homework-2026  # Automated
# OR
docker-compose exec dbt python3 /workspace/queries/homework_queries_2026.py
```

---

### Q4: Best Performing Green Zone in 2020
**Query**:
```sql
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green' AND pickup_year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

**Answer**: **East Harlem North** - $2,041,062.53

---

### Q5: Green Taxi Trips in October 2019
**Query**:
```sql
SELECT SUM(total_monthly_trips)
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND pickup_year = 2019
  AND pickup_month = 10;
```

**Answer**: **461,349 trips**

---

### Q6: FHV Records After Filtering
**Query**:
```sql
SELECT COUNT(*) FROM stg_fhv_tripdata;
```

**Answer**: **43,261,273 records**

*Note: Filter applied in staging model: `WHERE dispatching_base_num IS NOT NULL`*

---

## 🎯 Advanced Analytics (Bonus)

### Fare Percentiles - April 2020
Uses `PERCENTILE_CONT` window function for outlier detection:

| Service | P90 | P95 | P97 | Trip Count |
|---------|-----|-----|-----|------------|
| Green | $36.67 | $49.00 | $55.66 | 33,761 |
| Yellow | $22.69 | $32.00 | $39.47 | 230,503 |

**Skills**: Window functions, statistical analysis, outlier detection

---

### Top 5 Growing Zones - January 2020 (MoM %)
Uses `LAG` window function for month-over-month calculations:

1. **Breezy Point/Fort Tilden/Riis Beach** (Yellow) - +675.1%
2. **Heartland Village/Todt Hill** (Green) - +365.5%
3. **Arrochar/Fort Wadsworth** (Green) - +340.4%
4. **Financial District South** (Green) - +218.4%
5. **Saint George/New Brighton** (Green) - +217.7%

**Skills**: LAG window function, MoM growth calculations, growth metrics

---

## 🔧 Available Commands

### Quick Commands
```bash
make help           # Show all commands
make homework-all   # Run all homework queries
make homework-2026  # Run base homework (Q1-Q6)
make homework-2025  # Run advanced analytics
make verify-models  # Check all models built
make dbt-docs       # Generate & serve docs
```

### Docker Management
```bash
make build          # Build Docker image
make up             # Start container
make down           # Stop container
make shell          # Open bash in container
make logs           # Show container logs
```

### dbt Workflow
```bash
make dbt-deps       # Install dbt packages
make dbt-seed       # Load seed files
make dbt-run        # Build all models
make dbt-test       # Run all tests
make dbt-build      # deps + seed + run + test
make dbt-clean      # Remove dbt artifacts
```

### Cleanup & Reset
```bash
make dbt-clean      # Clean dbt artifacts only
make clean          # Remove container + volumes
make clean-data     # Remove downloaded data
make reset          # Reset containers (keeps data)
make reset-all      # FULL reset (removes everything)
```

---

## 📂 Project Structure

```
04-analytics-engineering/
├── queries/                         # Analysis queries
│   ├── homework_queries_2026.py     # Automated Q1-Q6
│   ├── homework_queries_2025.py     # Advanced analytics
│   ├── homework_2026.sql            # SQL version
│   └── manual_query.py              # Interactive tool
│
├── screenshots/                     # Project screenshots
│   ├── dbt_build_success.png
│   ├── q3_count_fct_monthly_zone_revenue.png
│   ├── q4_best_green_zone_2020.png
│   ├── q5_green_trips_oct_2019.png
│   ├── q6_fhv_records.png
│   ├── percentiles_april_2020.png
│   ├── top_5_growing_zones_jan_2020.png
│   ├── dbt_docs_lineage.png
│   └── duckdb_cli_query.png
│
├── taxi_rides_ny/                   # dbt project
│   ├── models/
│   │   ├── staging/                 # Raw → Clean (3 views)
│   │   ├── intermediate/            # Business logic (1 view)
│   │   ├── marts/                   # Analytics-ready (3 tables)
│   │   └── core/                    # Advanced analytics (3 tables)
│   ├── macros/                      # Reusable functions
│   │   ├── timestamp_diff.sql       # Cross-warehouse compatible
│   │   ├── day_of_week.sql
│   │   ├── get_payment_type_description.sql
│   │   └── get_rate_code_description.sql
│   ├── seeds/
│   │   └── taxi_zone_lookup.csv     # 265 NYC zones
│   └── tests/                       # Data quality tests
│
├── data/                            # Downloaded data (gitignored)
│   └── raw/                         # 60 parquet files
│
├── docker-compose.yml
├── Dockerfile
├── Makefile                         # 25+ automation commands
├── requirements.txt
└── README.md
```

---

## 🧪 Data Quality & Testing

### Test Coverage: 100%
```bash
make dbt-test
# Output: PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10
```

**Tests Implemented**:
- ✅ Unique constraints (trip_id, location_id)
- ✅ Not null constraints (required fields)
- ✅ Referential integrity (zones ↔ trips)
- ✅ Accepted values (payment types, rate codes)
- ✅ Business rules (valid dates, positive amounts)

---

## 🎓 Skills Demonstrated

### dbt & SQL
- ✅ Layered data architecture (staging → marts → core)
- ✅ Window functions (LAG, PERCENTILE_CONT, ROW_NUMBER)
- ✅ CTEs (Common Table Expressions)
- ✅ Cross-warehouse macros (DuckDB/BigQuery compatible)
- ✅ Jinja templating for dynamic SQL
- ✅ Comprehensive testing strategies
- ✅ Documentation generation (dbt docs)

### Software Engineering
- ✅ Docker containerization
- ✅ Makefile automation (25+ commands)
- ✅ Git version control
- ✅ Code reusability (macros)
- ✅ Professional project structure
- ✅ Comprehensive documentation

### Data Engineering
- ✅ ELT pipeline (Extract → Load → Transform)
- ✅ Data quality validation
- ✅ Performance optimization (materialization strategies)
- ✅ Large dataset processing (117M+ records)

---

## 📚 Running Homework Queries

### Method 1: Make Commands (Easiest)
```bash
make homework-all        # Run both 2026 + 2025
make homework-2026       # Base homework only
make homework-2025       # Advanced analytics only
```

### Method 2: Python Scripts
```bash
# Standard output
docker-compose exec dbt python3 /workspace/queries/homework_queries_2026.py

# Advanced analytics
docker-compose exec dbt python3 /workspace/queries/homework_queries_2025.py
```

### Method 3: SQL Files
```bash
docker-compose exec dbt bash
duckdb taxi_rides_ny.duckdb < /workspace/queries/homework_2026.sql
```

### Method 4: Interactive DuckDB CLI
```bash
# Install DuckDB CLI first
make install-duckdb-cli

# Then use interactively
docker-compose exec dbt bash
duckdb taxi_rides_ny.duckdb

D SELECT COUNT(*) FROM fct_monthly_zone_revenue;
D .quit
```

### Method 5: Python One-Liners
```bash
docker-compose exec dbt python3 -c "import duckdb; print(duckdb.connect('taxi_rides_ny.duckdb').execute('SELECT COUNT(*) FROM fct_trips').fetchone()[0])"
```

See [HOMEWORK_METHODS.md](documentation/HOMEWORK_METHODS.md) for detailed guide with all 6 methods.

---

## 🐛 Troubleshooting

### DuckDB CLI Not Found
```bash
make install-duckdb-cli
```

### Port 38080 Already in Use
```bash
# Find process
lsof -i :38080

# Change port in docker-compose.yml
ports:
  - "48080:8080"
```

### Makefile Tab Errors
```bash
# Fix tabs automatically
./fix_makefile_tabs.sh

# Or configure VSCode
# Add to settings.json:
"[makefile]": {
    "editor.insertSpaces": false
}
```

### Data Download Fails
```bash
# Manual download
docker-compose exec dbt python3 /workspace/download_data.py
```

---

## 📸 Screenshots

All homework results with formatted outputs available in `screenshots/` directory:

- ✅ dbt build success
- ✅ Q3-Q6 query results
- ✅ Advanced analytics (percentiles, growth)
- ✅ dbt docs lineage graph
- ✅ DuckDB CLI queries

---

## 🌐 Documentation

- **README.md** - This file
- **HOMEWORK_METHODS.md** - 6 ways to run queries
- **CLEANING.md** - Cleanup & reset guide
- **TROUBLESHOOTING.md** - Common issues
- **dbt docs** - `make dbt-docs` → http://localhost:38080

---

## 🔗 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [NYC TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)

---

## 💼 Portfolio Value

This project demonstrates:

✅ **Modern Data Stack**: dbt, DuckDB, Docker
✅ **Production-Grade Code**: Testing, documentation, automation
✅ **Large-Scale Processing**: 117M+ records efficiently
✅ **Advanced SQL**: Window functions, CTEs, complex joins
✅ **Software Engineering**: Clean architecture, reusable code
✅ **DevOps**: Docker, Makefile, reproducible workflows

**Ready for production** and **impressive in interviews**.

---

## ✨ Author

**Ellie Pascaud**
- Transitioning: Financial Controller → Analytics Engineer
- Skills: dbt, SQL, Python, Docker, Data Modeling
- Contact: ellie.pascaud@gmail.com

---

## 📜 License

Educational project for Data Engineering Zoomcamp 2026

---

**⭐ Star this project if it helped you!**

*Built with ❤️ for learning Analytics Engineering*
