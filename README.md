# Pipeline ETL – Open-Meteo

Pipeline de données météorologiques pour Lille : ingestion depuis l’API [Open-Meteo](https://open-meteo.com/), transformation, puis stockage dans un entrepôt PostgreSQL.

Le cas d’usage est un suivi horaire de la météo (température, humidité, vent) afin d’historiser des observations propres, requêtables et sans doublon. L’orchestration est assurée par Apache Airflow ; l’environnement complet tourne sous Docker Compose.

## Architecture technique

Les composants suivants remplacent une stack Hadoop (HDFS / YARN) : un data lake local pour les fichiers, Airflow pour l’orchestration, PostgreSQL pour le stockage.

```text
┌─────────────────┐
│  Open-Meteo API │  source externe (forecast, current weather)
└────────┬────────┘
         │  HTTP
         ▼
┌─────────────────┐     ┌──────────────────────────────────────┐
│  extract.py     │────►│  Data lake local                     │
│  (ingestion)    │     │  data/raw/weather_YYYYMMDD_HHMMSS.json│
└─────────────────┘     └──────────────────┬───────────────────┘
                                           │
                                           ▼
                            ┌─────────────────────────┐
                            │  transform.py (Pandas)  │
                            │  data/processed/        │
                            │  weather.csv            │
                            └────────────┬────────────┘
                                         │
                                         ▼
                            ┌─────────────────────────┐
                            │  load.py                │
                            │  PostgreSQL 16          │
                            │  table weather          │
                            │  (entrepôt / DWH)       │
                            └─────────────────────────┘
```

Orchestration Airflow 3.3.1 (`DAG weather_pipeline`, schedule `@hourly`) :

```text
extract → transform → load
```

| Service Compose | Conteneur | Rôle |
| --- | --- | --- |
| `postgres` | `enguerran-weather-postgres` | Entrepôt de données (`weather`), port hôte **5445** |
| `airflow-db` | `enguerran-airflow-db` | Métadonnées Airflow |
| `airflow-init` | `enguerran-airflow-init` | Migrations de la base Airflow |
| `airflow-apiserver` | `enguerran-airflow-apiserver` | API + interface web (`http://localhost:8181`) |
| `airflow-scheduler` | `enguerran-airflow-scheduler` | Planification et exécution (`LocalExecutor`) |
| `airflow-dag-processor` | `enguerran-airflow-dag-processor` | Parsing des DAGs |
| GitHub Actions | — | CI/CD : Pytest, puis déploiement SSH sur `main` |

Les **services** Compose (`postgres`, `airflow-db`, `airflow-apiserver`, …) restent les hostnames du réseau Docker (`postgres:5432`, `airflow-db:5432`, `http://airflow-apiserver:8080/execution/`). Les **`container_name`** sont préfixés `enguerran-` pour éviter les collisions sur une VM partagée.

Le chargement est idempotent : contrainte `UNIQUE(latitude, longitude, observation_time)` et `ON CONFLICT DO NOTHING`.

## Prérequis

| Outil | Version |
| --- | --- |
| Docker | 24+ (Engine + Compose v2) |
| Python | 3.12+ en local (CI : **3.13**) |
| Apache Airflow (image) | **3.3.1** (`pipeline-airflow:3.3.1`) |
| PostgreSQL (image) | **16** |
| Git | 2.x |
| Accès réseau | API Open-Meteo (`https://api.open-meteo.com`) |

Dépendances Python (`requirements.txt`) : `requests`, `pandas`, `sqlalchemy`, `psycopg2-binary`. Pytest est installé en plus pour les tests.

## Instructions d'installation et de déploiement

### 1. Cloner le dépôt

```bash
git clone https://github.com/EnguerranSGG/pipeline-data.git
cd pipeline-data
```

### 2. Créer le fichier d’environnement

Créer un fichier `.env` à la racine (non versionné) :

```bash
POSTGRES_DB=weather
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_JWT_SECRET=$(openssl rand -hex 64)
```

`AIRFLOW_JWT_SECRET` doit être **identique** pour tous les services Airflow (sinon les tâches échouent avec `Invalid auth token`).

### 3. Construire l’image Airflow

L’image installe les librairies ETL par-dessus `apache/airflow:3.3.1` :

```bash
docker build -t pipeline-airflow:3.3.1 .
```

### 4. Démarrer la stack

```bash
docker compose up -d
docker compose ps
```

Conteneurs attendus : `enguerran-weather-postgres`, `enguerran-airflow-db`, `enguerran-airflow-apiserver`, `enguerran-airflow-scheduler`, `enguerran-airflow-dag-processor`. `enguerran-airflow-init` se termine avec le code 0 après les migrations.

### 5. Vérifier l’interface Airflow

Ouvrir [http://localhost:8181](http://localhost:8181). Le DAG `weather_pipeline` doit apparaître (Simple Auth Manager, accès admin ouvert en local).

### 6. Arrêter l’environnement

```bash
docker compose down
```

Les volumes `postgres_data` et `airflow_db_data` conservent les données. Pour tout réinitialiser : `docker compose down -v`.

## Guide d'exécution

### A. Pipeline manuel (hors Airflow)

Utile pour déboguer une étape.

```bash
# Ingestion : écrit data/raw/weather_*.json
python src/extract.py

# Traitement : lit le JSON le plus récent, écrit data/processed/weather.csv
python src/transform.py

# Stockage : insert dans PostgreSQL (conteneur déjà démarré)
# DATABASE_URL par défaut : postgresql://airflow:airflow@localhost:5445/weather
python src/load.py
```

Contrôle SQL :

```bash
psql -h localhost -p 5445 -U airflow -d weather -c "SELECT * FROM weather ORDER BY loaded_at DESC LIMIT 5;"
```

### B. Pipeline orchestré (Airflow)

1. Démarrer Docker Compose (voir ci-dessus).
2. Aller sur [http://localhost:8181](http://localhost:8181).
3. Activer le DAG `weather_pipeline`.
4. Lancer un run manuel (**Trigger DAG**) ou attendre le cron horaire `@hourly`.

Les tâches s’enchaînent : `extract` → `transform` → `load`. Les fichiers sont montés dans `/opt/airflow/data` via `DATA_DIR`.

Logs d’un service :

```bash
docker logs enguerran-airflow-scheduler --tail 50
docker logs enguerran-airflow-apiserver --tail 50
```

### C. Tests

```bash
python -m pip install -r requirements.txt
python -m pip install pytest
pytest -v
```

## Captures d'écran / logs de validation

### Services Docker

```text
NAME                               IMAGE                    STATUS         PORTS
enguerran-airflow-apiserver        pipeline-airflow:3.3.1   Up             0.0.0.0:8181->8080/tcp
enguerran-airflow-dag-processor    pipeline-airflow:3.3.1   Up             8080/tcp
enguerran-airflow-db               postgres:16              Up             5432/tcp
enguerran-airflow-scheduler        pipeline-airflow:3.3.1   Up             8080/tcp
enguerran-weather-postgres         postgres:16              Up             0.0.0.0:5445->5432/tcp
```

### Tests Pytest

```text
tests/test_transform.py::test_weather_columns PASSED
tests/test_transform.py::test_weather_values PASSED
============================== 2 passed in 0.72s ===============================
```

Les tests contrôlent les colonnes, l’absence de valeurs manquantes, une humidité entre 0 et 100 %, des coordonnées valides et une vitesse de vent ≥ 0.

### Données en base (entrepôt PostgreSQL)

```sql
SELECT id, latitude, longitude, observation_time, temperature, humidity, wind_speed, loaded_at
FROM weather
ORDER BY loaded_at DESC
LIMIT 5;
```

Exemple de résultat réel :

```text
 id | latitude | longitude |  observation_time   | temperature | humidity | wind_speed |         loaded_at
----+----------+-----------+---------------------+-------------+----------+------------+----------------------------
  4 |   50.638 | 3.0449998 | 2026-09-10 11:00:00 |        18.2 |       56 |       10.4 | 2026-09-10 11:00:04.653495
  3 |   50.638 | 3.0449998 | 2026-09-10 09:45:00 |        17.4 |       61 |          9 | 2026-09-10 09:56:29.189243
  1 |   50.638 | 3.0449998 | 2026-09-10 08:00:00 |        14.1 |       75 |       10.1 | 2026-09-10 08:25:18.461596
```

### CI GitHub Actions

Workflow `.github/workflows/ci.yml` : checkout, Python 3.13, `pip install`, `pytest -v`.

Après des tests réussis, un job **Deploy** s’exécute uniquement sur un **push** vers `main` (pas sur une pull request). Il se connecte en SSH à la VM, fait `git pull --ff-only` dans `/opt/pipeline-data`, puis `docker compose up -d`. Les identifiants SSH sont des secrets GitHub (`SERVER_HOST`, `SERVER_USER`, `SERVER_SSH_KEY`). Le fichier `.env` reste sur la VM et n’est jamais versionné.

Run `ci: add GitHub Actions pytest workflow #2` : job **tests** en succès (21 s).

![GitHub Actions — job tests du workflow CI Pipeline en succès](images/ci-github-actions.png)

### Interface Airflow

DAG `weather_pipeline` listé et actif sur [http://localhost:8181](http://localhost:8181) : dernière exécution réussie le 10/09/2026 à 13:00, prochaine à 14:00.

![Liste des DAGs Airflow — weather_pipeline actif](images/dag-interface.png)

Run planifié `scheduled__2026-09-10T11:00:00+00:00` en **Succès** : les trois tâches `extract`, `transform` et `load` sont au vert (durée totale ~4 s).

![Exécution du DAG weather_pipeline — extract, transform et load en succès](images/dag-succes.png)

Déploiement sur la VM Ubuntu : interface Airflow joignable à `http://81.17.94.238:8181`, DAG `weather_pipeline` listé et actif.

![Airflow déployé sur la VM — weather_pipeline accessible sur le port 8181](images/interface-deployee.png)

## Structure du projet

```text
pipeline-data/
├── dags/pipeline_dag.py
├── src/extract.py
├── src/transform.py
├── src/load.py
├── data/raw/
├── data/processed/
├── tests/test_transform.py
├── sql/init.sql
├── .github/workflows/ci.yml
├── images/
│   ├── ci-github-actions.png
│   ├── dag-interface.png
│   ├── dag-succes.png
│   └── interface-deployee.png
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```
