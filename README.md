# ODIN Mobility – Data Engineering Challenge

This project implements a complete data engineering pipeline for the CBS mobility dataset:

- a relational data model in PostgreSQL,
- an ETL pipeline loading the data,
- analytical SQL queries answering business questions (Task 3).

The entire pipeline runs locally using **Docker Compose**.

---

## Requirements

- Docker CLI
- Docker Compose v2 (`docker compose`)
- **Colima 0.9.1** (tested) or Docker Desktop

If you are using Colima on macOS, start it first:
```bash
colima start
```

No local Python or PostgreSQL installation is required.

---

## Project Layout
```
.
├── config/                 # Configuration files
├── data/                   # Input data files
├── src/                    # Python ETL source code
├── sql/                    # SQL schema and query files
│   ├── dim_tables.sql
│   ├── fact_table.sql
│   └── tasks/              # Task 3 analytical queries
│       ├── task3_query1.sql
│       ├── task3_query2.sql
│       ├── task3_query3.sql
│       └── task3_query4.sql
├── test/                   # Test suite
├── docker-compose.yml      # Local orchestration
├── dockerfile              # ETL container build instructions
├── task-definition.json    # AWS ECS task definition
├── pyproject.toml          # Python project configuration
├── uv.lock                 # Dependency lock file
└── README.md
```

---

## Run Everything with Docker

### 1. Load the ETL image (if provided as `.tar`)
```bash
docker load -i odin-etl-revo_v1.0.0.tar
```

### 2. Create the environment file
```bash
cp .env.example .env
```

The `.env` file contains runtime configuration only and is not committed to version control.

### 3. Start the full pipeline

This single command will:

- start PostgreSQL,
- initialize schema and tables,
- run the ETL pipeline.
```bash
docker compose up --abort-on-container-exit --exit-code-from etl
```

All logs are printed directly to the console.

### 4. Re-run from scratch (optional)

To reset the database and replay the full pipeline:
```bash
docker compose down -v
docker compose up --abort-on-container-exit --exit-code-from etl
```

---

## Running Task 3 Analytical Queries

Task 3 SQL files are mounted into a dedicated runner container (`task3`) at:
```
/sql/tasks
```

### Run queries from the task3 container

Start the stack first:
```bash
docker compose up -d
```

Open a shell in the Task 3 container:
```bash
docker exec -it odin-task3 sh
```

Run queries using `psql`:
```bash
psql -h db -U ${USERNAME} -d ${DATABASE} -f /sql/tasks/task3_query1.sql
psql -h db -U ${USERNAME} -d ${DATABASE} -f /sql/tasks/task3_query2.sql
psql -h db -U ${USERNAME} -d ${DATABASE} -f /sql/tasks/task3_query3.sql
psql -h db -U ${USERNAME} -d ${DATABASE} -f /sql/tasks/task3_query4.sql
```