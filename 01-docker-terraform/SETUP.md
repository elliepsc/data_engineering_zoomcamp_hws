# Setup Guide - Module 1

## Quick Start (5 minutes)

```bash
# 1. Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Setup project
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# 3. Configure
cp .env.example .env

# 4. Start database
docker compose up -d

# 5. Download data
mkdir -p data
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet -P data/
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv -P data/

# 6. Run pipeline
uv run python -m pipeline.cli ingest
```

## Detailed Setup

### Prerequisites

**Required:**
- Python 3.11 or higher
- Docker Desktop
- 2GB free disk space

**Recommended:**
- `uv` package manager (10-100x faster than pip)
- VS Code with Python extension

### Step 1: Install uv

**Linux/Mac:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Verify:**
```bash
uv --version
```

### Step 2: Create Environment

```bash
# Create virtual environment
uv venv

# Activate (Linux/Mac)
source .venv/bin/activate

# Activate (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate (Windows Git Bash)
source .venv/Scripts/activate
```

### Step 3: Install Dependencies

```bash
uv pip install -r requirements.txt
```

**Dependencies installed:**
- pandas (data manipulation)
- pyarrow (parquet reading)
- sqlalchemy (database ORM)
- psycopg2-binary (PostgreSQL driver)
- python-dotenv (env variable loading)
- click (CLI framework)
- pytest + pytest-cov (testing)
- jupyter (notebooks)

### Step 4: Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit if needed (default values work)
nano .env  # or your editor
```

**Default configuration:**
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=ny_taxi
DATA_DIR=data
```

### Step 5: Start Database

```bash
# Start PostgreSQL + pgAdmin
docker compose up -d

# Verify running
docker compose ps

# Check logs
docker compose logs -f postgres
```

**Services started:**
- PostgreSQL on port 5433 (mapped from internal 5432)
- pgAdmin on port 8080

### Step 6: Download Data

```bash
# Create data directory
mkdir -p data

# Download green taxi trips (Nov 2025)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet -P data/

# Download taxi zones
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv -P data/

# Verify downloads
ls -lh data/
```

### Step 7: Run Pipeline

**Option 1: CLI (Recommended)**
```bash
uv run python -m pipeline.cli ingest
```

**Option 2: Simple Script**
```bash
uv run python -m pipeline.ingest_pipeline
```

### Step 8: Verify

```bash
# Check data via CLI
uv run python -m pipeline.cli status
uv run python -m pipeline.cli verify green_taxi_trips

# Or use pgAdmin
# Open http://localhost:8080
# Email: pgadmin@pgadmin.com
# Password: pgadmin
```

## Troubleshooting

### Issue: Cannot connect to database

**Error:** `psycopg2.OperationalError: could not connect`

**Solutions:**
1. Check Docker is running: `docker compose ps`
2. Verify port mapping: `docker compose port postgres 5432`
3. Check .env: `POSTGRES_HOST=localhost` and `POSTGRES_PORT=5433`
4. Wait 10 seconds after `docker compose up` (PostgreSQL starting)

### Issue: File not found

**Error:** `FileNotFoundError: data/green_tripdata_2025-11.parquet`

**Solution:**
```bash
# Download data files
mkdir -p data
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet -P data/
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv -P data/
```

### Issue: Permission denied (Windows)

**Error:** `PermissionError: [WinError 5] Access is denied`

**Solution:**
- Run terminal as Administrator
- Or use Git Bash instead of Command Prompt

### Issue: uv not found

**Error:** `bash: uv: command not found`

**Solution:**
```bash
# Add to PATH (Linux/Mac)
export PATH="$HOME/.cargo/bin:$PATH"

# Or restart terminal after installation
```

## Testing

```bash
# Run all tests
uv run pytest -v --cov=pipeline

# Unit tests only (no database)
uv run pytest -v -m "not integration"

# Integration tests (requires docker compose up)
uv run pytest -v -m integration
```

## Cleanup

```bash
# Stop containers
docker compose down

# Remove volumes (deletes data)
docker compose down -v

# Deactivate environment
deactivate
```

## Next Steps

1. **Explore data:** Open `notebooks/01_data_exploration.ipynb`
2. **Read decisions:** See [DECISIONS.md](DECISIONS.md)
3. **Learning notes:** See [DEVLOG.md](DEVLOG.md)
4. **Run homework queries:** See [README.md](README.md#homework-solutions)

## Support

- **Course:** Data Engineering Zoomcamp Slack
- **Issues:** GitHub Issues (if applicable)
- **Docs:** See README.md for detailed documentation
