# Spark Data Processing Pipeline

Кластер Apache Spark, развернутый через Docker Compose.

- 1 Spark Master
- 3 Spark Worker
- JupyterLab с PySpark

Версия Spark: 3.5.0

## Запуск

```bash
docker compose up -d
```

## Адреса

| Сервис | Адрес |
|---|---|
| Spark Master UI | http://localhost:8080 |
| JupyterLab | http://localhost:8888 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| Worker 3 | http://localhost:8083 |

## Запуск тестового задания

```bash
docker cp jobs/test_job.py spark-master:/opt/spark/test_job.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/test_job.py
```

## Остановка

```bash
docker compose down
```
