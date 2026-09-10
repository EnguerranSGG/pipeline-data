# Rapport d’analyse — captures du pipeline ETL Open-Meteo

Ce document présente uniquement les preuves visuelles du pipeline : DAG Airflow, exécutions réussies, déploiement et résultats de validation.

DAG : `weather_pipeline`  
Enchaînement : `extract` → `transform` → `load`  
Planification : `@hourly`

---

## 1. Pipeline et DAG Airflow (local)

Le DAG `weather_pipeline` est chargé dans l’interface Airflow. Il est **actif**, tagué `etl` / `weather`.

- Dernière exécution réussie : **2026-09-10 13:00** (coche verte)
- Prochaine exécution planifiée : **2026-09-10 14:00**
- Historique des runs visible à droite (barres vertes = succès)

![Liste des DAGs — weather_pipeline actif](../images/dag-interface.png)

---

## 2. Résultat d’une exécution du pipeline

Run planifié `scheduled__2026-09-10T11:00:00+00:00` : état global **Succès**.

| Tâche | Opérateur | État | Début | Durée |
| --- | --- | --- | --- | --- |
| `extract` | PythonOperator | Succès | 13:00:01 | 1,401 s |
| `transform` | PythonOperator | Succès | 13:00:03 | 0,306 s |
| `load` | PythonOperator | Succès | 13:00:04 | 0,492 s |

Durée totale du DAG : **4,290 s**. Les trois étapes ETL se sont enchaînées sans échec.

![Instances de tâches — extract, transform et load en succès](../images/dag-succes.png)

---

## 3. DAG déployé sur la VM

Même interface, exposée sur la VM Ubuntu partagée : `http://81.17.94.238:8181/dags`.

Le DAG `weather_pipeline` est listé et **actif** (interrupteur à droite). Le port hôte **8181** correspond au mapping `8181:8080` (Jenkins conserve le 8080).

![Airflow sur la VM — weather_pipeline accessible](../images/interface-deployee.png)

---

## 4. Résultat CI (validation du traitement)

Workflow GitHub Actions `CI Pipeline`, run `ci: add GitHub Actions pytest workflow #2`.

Job **tests** : **succès** en 21 s (checkout, Python, dépendances, `pytest`).

![GitHub Actions — job tests en succès](../images/ci-github-actions.png)
