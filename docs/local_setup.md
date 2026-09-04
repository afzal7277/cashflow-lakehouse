# Local Setup — Cashflow Lakehouse (Docker)

## 1. Prerequisites
- Docker Desktop installed and running
- VS Code with the **Dev Containers** extension (optional but recommended)

## 2. Build the Image
From the project root:
```powershell
docker compose build
```

## 3. Start the Container
```powershell
docker compose up -d
docker compose exec cashflow-lakehouse bash
```
You're now inside the container with Python 3.11 + JDK 17 + all
dependencies installed, and your project folder live-mounted at `/app`
(edits on your host machine reflect immediately, no rebuild needed).

## 4. Sanity Check
Inside the container:
```bash
python -c "from configs.spark_session import get_spark_session; s = get_spark_session(); print(s.version)"
```
Should print the Spark version with no errors.

## 5. Environment Variables
Copy `.env.example` to `.env` and fill in values as needed (Azure/Databricks
fields stay blank until the cluster-scale phase). `.env` is gitignored.
```powershell
copy .env.example .env
```

## 6. Get the Seed Dataset
Download PaySim1 from Kaggle:
`https://www.kaggle.com/datasets/ealaxi/paysim1`

Extract and place the CSV at:
```
data/seed/paysim.csv
```
(on your host machine — it's live-mounted into the container automatically)

## 7. Run the Data Generator
Inside the container, start small first:
```bash
NUM_ROWS=100000 python generator/generate_transactions.py
```
Progress and errors log to console and to `logs/generator.log`. If a run
fails partway, just re-run the same command — it resumes from the last
completed batch using `logs/generator_checkpoint.json`.

Once confirmed, scale up (e.g. 5M rows) or move to the cluster for the full
20-30GB run.

## 8. Stopping
```powershell
docker compose down
```

## 9. Project Structure Reference
See `docs/HLD.md` and `docs/LLD.md` for architecture and module details.
