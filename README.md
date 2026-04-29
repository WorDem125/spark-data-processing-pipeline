# Spark Cluster — Docker Compose

Кластер: 1 Spark Master + 3 Spark Worker (bitnami/spark:3.5)

## Быстрый старт

```bash
docker compose up -d
```

Веб-интерфейс Master: http://localhost:8080

## Запуск тестового задания

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/test_job.py
```

## Остановка

```bash
docker compose down
```
