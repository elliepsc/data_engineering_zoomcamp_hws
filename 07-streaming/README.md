# Module 7 — Stream Processing with Kafka (Redpanda) + PyFlink

Data Engineering Zoomcamp 2026 | Green Taxi Trip Data — October 2025

## Stack

- Redpanda v25.3.9 (Kafka-compatible broker)
- Apache Flink 2.2.0
- PyFlink (Python API for Flink)
- PostgreSQL 18
- Python 3.12 + kafka-python + pandas + pyarrow

## Project Structure

```
07-streaming/
├── .venv/                          # Virtual environment (gitignored)
└── workshop/
    ├── data/                       # Parquet data files (gitignored)
    ├── src/
    │   ├── consumers/              # Workshop original consumers (rides topic)
    │   ├── producers/              # Workshop original producers (rides topic)
    │   ├── job/
    │   │   ├── hw/                 # Homework Flink jobs + producer/consumer
    │   │   │   ├── producer.py     # Green taxi producer (green-trips topic)
    │   │   │   ├── consumer.py     # Green taxi consumer (green-trips topic)
    │   │   │   ├── job_q4_tumbling_pickup.py
    │   │   │   ├── job_q5_session_window.py
    │   │   │   └── job_q6_tumbling_tips.py
    │   │   ├── aggregation_job.py  # Workshop original jobs
    │   │   └── pass_through_job.py
    │   └── models.py
    ├── docker-compose.yml
    └── Dockerfile.flink
```

> **Important**: Les jobs Flink doivent être dans `src/job/hw/` car ce dossier est monté dans le container via le volume `./src/:/opt/src`. Toute modification doit se faire dans `src/job/hw/`, pas ailleurs.

## Setup

### 1. Ports — conflits à éviter

Le `docker-compose.yml` original utilise les ports 8081 et 8082, potentiellement occupés par d'autres services. Modifie avant de lancer :

```yaml
redpanda:
  ports:
    - 8085:8082    # était 8082:8082

jobmanager:
  ports:
    - "8083:8081"  # était 8081:8081
```

Le Flink UI sera accessible sur http://localhost:8083

### 2. Environnement Python

```bash
cd 07-streaming
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python pandas pyarrow
```

### 3. Lancer l'infrastructure

```bash
cd workshop
docker compose build
docker compose up -d
docker compose ps
```

Services démarrés :
- Redpanda (Kafka) : `localhost:9092`
- Flink Job Manager : `http://localhost:8083`
- PostgreSQL : `localhost:5432`

### 4. Télécharger les données

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet -P data/
```

Dataset : **49,416 lignes** de courses Green Taxi octobre 2025.

### 5. Créer le topic Redpanda

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

### 6. Créer les tables PostgreSQL

```bash
docker exec -it workshop-postgres-1 psql -U postgres -d postgres -c "
CREATE TABLE tumbling_window_trips (
    window_start TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
CREATE TABLE session_window_trips (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
CREATE TABLE tumbling_window_tips (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);"
```

---

## Questions 1–3 : Kafka Producer/Consumer

### Q1 — Version Redpanda

```bash
docker exec -it workshop-redpanda-1 rpk version
```

**Réponse : v25.3.9** ✅

### Q2 — Envoi des données

Le producer (`src/job/hw/producer.py`) lit le parquet et envoie chaque ligne au topic `green-trips`.

**Fix important** : Le dataset contient des valeurs `NaN` dans `passenger_count`. Flink ne supporte pas `NaN` en JSON. On remplace par `None` (sérialisé en `null`) avant l'envoi :

```python
df = df.where(pd.notnull(df), None)
```

```bash
python3 src/job/hw/producer.py
```

**Réponse : 10 seconds** ✅ (~6-8s en pratique, dans la tranche "10 seconds")

### Q3 — Trips > 5km

```bash
python3 src/job/hw/consumer.py
```

**Réponse : 8506** ✅

---

## Questions 4–6 : PyFlink

### Fix commun à tous les jobs

Flink 2.x ne supporte pas `NaN` en JSON même avec le fix producer (anciens messages dans le topic). Tous les jobs source incluent :

```python
'json.ignore-parse-errors' = 'true'
```

### Commande générique pour soumettre un job

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/<job_file>.py
```

### Reset propre entre les jobs

```bash
# Supprimer et recréer le topic (évite les doublons)
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
docker exec -it workshop-redpanda-1 rpk topic create green-trips
python3 src/job/hw/producer.py

# Vider la table PostgreSQL concernée
docker exec -it workshop-postgres-1 psql -U postgres -d postgres -c "TRUNCATE <table>;"
```

### Q4 — Tumbling Window 5 min par PULocationID

Job : `src/job/hw/job_q4_tumbling_pickup.py`

Fenêtre glissante de 5 minutes, compte les trips par `PULocationID`.

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q4_tumbling_pickup.py
```

```sql
SELECT PULocationID, num_trips
FROM tumbling_window_trips
ORDER BY num_trips DESC
LIMIT 3;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
```

**Réponse : 74** ✅

### Q5 — Session Window par PULocationID

Job : `src/job/hw/job_q5_session_window.py`

Fenêtre de session avec gap de 5 minutes, partitionnée par `PULocationID`.

#### ⚠️ Piège : PARTITION BY obligatoire

**Sans `PARTITION BY`** : Flink crée une seule session globale — dès qu'un event arrive n'importe où, la fenêtre reste ouverte pour toutes les locations. Résultat : sessions aberrantes avec 400+ trips.

**Avec `PARTITION BY PULocationID`** : Flink crée une session indépendante par location — la fenêtre de la location 74 se ferme après 5 minutes sans activité à cette location spécifiquement. Résultat : sessions réalistes.

```sql
FROM TABLE(
    SESSION(TABLE green_trips PARTITION BY PULocationID,
            DESCRIPTOR(event_timestamp),
            INTERVAL '5' MINUTE)
)
```

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q5_session_window.py
```

```sql
SELECT PULocationID, num_trips
FROM session_window_trips
ORDER BY num_trips DESC
LIMIT 3;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |        81
           74 |        72
           74 |        71
```

**Réponse : 81** ✅

### Q6 — Tumbling Window 1h — Total Tips

Job : `src/job/hw/job_q6_tumbling_tips.py`

Fenêtre d'1 heure, somme des `tip_amount` sur toutes les locations.

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/hw/job_q6_tumbling_tips.py
```

```sql
SELECT window_start, total_tip
FROM tumbling_window_tips
ORDER BY total_tip DESC
LIMIT 3;
```

```
    window_start     |     total_tip
---------------------+--------------------
 2025-10-16 18:00:00 |  510.86
 2025-10-30 16:00:00 |  494.41
 2025-10-09 18:00:00 |  472.01
```

**Réponse : 2025-10-16 18:00:00** ✅

---

## Résumé des réponses

| # | Question | Réponse |
|---|----------|---------|
| 1 | Redpanda version | **v25.3.9** |
| 2 | Temps d'envoi des données | **10 seconds** |
| 3 | Trips avec distance > 5km | **8506** |
| 4 | PULocationID le plus fréquent (tumbling 5min) | **74** |
| 5 | Trips dans la plus longue session | **81** |
| 6 | Heure avec le plus de tips | **2025-10-16 18:00:00** |

## Troubleshooting

**TaskManager down** : Le taskmanager peut crasher si trop de jobs échouent en boucle (OOM). Solution :
```bash
docker compose down
docker compose up -d
```

**Topic introuvable après restart** : `docker compose down` supprime les volumes. Recréer le topic et renvoyer les données.

**NaN dans JSON** : Toujours ajouter `df = df.where(pd.notnull(df), None)` dans le producer ET `'json.ignore-parse-errors' = 'true'` dans le DDL source Flink.
