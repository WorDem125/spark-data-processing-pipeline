# Тестовое задание для проверки работы Spark-кластера.
# Запуск: spark-submit --master spark://spark-master:7077 test_job.py

import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum as _sum

spark = (
    SparkSession.builder
    .appName("TestClusterJob")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Версия Spark : {spark.version}")
print(f"Master URL   : {spark.sparkContext.master}")

# Тест 1: агрегация датафрейма
data = [
    ("Alice",  "Engineering", 95000),
    ("Bob",    "Engineering", 88000),
    ("Carol",  "Marketing",   72000),
    ("David",  "Marketing",   68000),
    ("Eve",    "HR",          61000),
    ("Frank",  "HR",          59000),
    ("Grace",  "Engineering", 102000),
    ("Heidi",  "Marketing",   75000),
]

df = spark.createDataFrame(data, ["name", "department", "salary"])
df.show()

df.groupBy("department").agg(
    count("name").alias("employees"),
    avg("salary").alias("avg_salary"),
    _sum("salary").alias("total_salary"),
).orderBy("department").show()

# Тест 2: параллельные вычисления методом Монте-Карло
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

print(f"Оценка числа pi: {4.0 * count_inside / NUM_SAMPLES:.5f}")

spark.stop()
