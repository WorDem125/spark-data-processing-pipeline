"""
Тестовое PySpark-задание для проверки кластера.
Запускается через spark-submit из контейнера spark-master.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum

spark = (
    SparkSession.builder
    .appName("TestClusterJob")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("  Spark-кластер работает!")
print(f"  Версия Spark: {spark.version}")
print(f"  Приложение: {spark.sparkContext.appName}")
print(f"  Master URL: {spark.sparkContext.master}")
print("=" * 60 + "\n")

data = [
    ("Alice",   "Engineering", 95000),
    ("Bob",     "Engineering", 88000),
    ("Carol",   "Marketing",   72000),
    ("David",   "Marketing",   68000),
    ("Eve",     "HR",          61000),
    ("Frank",   "HR",          59000),
    ("Grace",   "Engineering", 102000),
    ("Heidi",   "Marketing",   75000),
]

df = spark.createDataFrame(data, ["name", "department", "salary"])

print("── Исходный датафрейм ──────────────────────────────────────")
df.show()

print("── Статистика по отделам ───────────────────────────────────")
(df.groupBy("department")
   .agg(
       count("name").alias("employees"),
       avg("salary").alias("avg_salary"),
       _sum("salary").alias("total_salary"),
   )
   .orderBy("department")
   .show())

print("── Параллельный подсчёт числа π (метод Монте-Карло) ────────")
import random

NUM_SAMPLES = 1_000_000

def inside(_):
    x, y = random.random(), random.random()
    return x * x + y * y < 1.0

count_inside = (
    spark.sparkContext
    .parallelize(range(NUM_SAMPLES), numSlices=4)
    .filter(inside)
    .count()
)
pi_estimate = 4.0 * count_inside / NUM_SAMPLES
print(f"  Оценка числа π ≈ {pi_estimate:.5f}  (точное значение: 3.14159…)\n")

print("=" * 60)
print("  Тестовое задание выполнено успешно!")
print("=" * 60 + "\n")

spark.stop()
