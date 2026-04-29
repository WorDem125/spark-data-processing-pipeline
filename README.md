# Spark Data Processing Pipeline

Сервис распределённой обработки и аналитики данных на базе Apache Spark.  
Проект разворачивается одной командой — кластер, Jupyter и все зависимости подняты в Docker Compose.

---

## Стек технологий

| Инструмент | Роль |
|---|---|
| Apache Spark 3.5.0 | Движок распределённой обработки данных |
| PySpark | Python API для работы с Spark |
| Docker Compose | Оркестрация контейнеров |
| JupyterLab | Среда разработки для PySpark-ноутбуков |
| Parquet | Колоночный формат хранения обработанных данных |

---

## Архитектура кластера

```
JupyterLab  (driver — точка входа, здесь пишется и запускается код)
     |
     |  spark://spark-master:7077
     |
Spark Master  (управляет кластером, распределяет задачи)
     |
 ____|____
|    |    |
W1  W2   W3   ← Spark Workers (executors — выполняют вычисления)
```

Все компоненты работают в изолированной Docker-сети и видят друг друга по именам сервисов.

---

## Структура проекта

```
spark-data-processing-pipeline/
├── data/
│   ├── raw/                                    # Исходные данные (CSV)
│   └── processed/
│       └── videogame_sales_parquet/            # Результат обработки (Parquet)
├── jobs/
│   └── test_job.py                             # Smoke-тест кластера через spark-submit
├── notebooks/
│   └── 01_pyspark_data_processing.ipynb       # Основной ноутбук с пайплайном
├── screenshots/                                # Скриншоты для отчёта
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

## Быстрый старт

### 1. Запустить кластер

```bash
docker compose up -d
```

Команда поднимает 5 контейнеров: Spark Master, три Worker-узла и JupyterLab.  
При первом запуске Docker скачает образы (~1 ГБ), последующие запуски — мгновенные.

### 2. Убедиться, что всё запущено

```bash
docker compose ps
```

Все контейнеры должны быть в статусе `Up`. Master и Jupyter дополнительно показывают `(healthy)`.

### 3. Открыть интерфейсы

| Сервис | Адрес |
|---|---|
| Spark Master UI | http://localhost:8080 |
| JupyterLab | http://localhost:8888 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| Worker 3 | http://localhost:8083 |

### 4. Подключить ноутбук к ядру Jupyter

**Вариант А — через браузер (рекомендуется)**

1. Открыть [http://localhost:8888](http://localhost:8888) — JupyterLab откроется без пароля
2. В файловом менеджере слева перейти в папку `work/`
3. Открыть `01_pyspark_data_processing.ipynb`
4. Ядро **Python 3 (ipykernel)** запустится автоматически

**Вариант Б — через VS Code**

1. Открыть файл `notebooks/01_pyspark_data_processing.ipynb` в VS Code
2. Нажать **Select Kernel** в правом верхнем углу
3. Выбрать **Existing Jupyter Server...**
4. Ввести адрес сервера: `http://localhost:8888`
5. Поле токена оставить **пустым**, нажать Enter
6. Выбрать ядро **Python 3 (ipykernel)**

### 5. Запустить ноутбук

Запустить все ячейки последовательно сверху вниз.  
Первая ячейка создаёт `SparkSession` и подключается к кластеру — после её выполнения в Spark Master UI ([http://localhost:8080](http://localhost:8080)) в разделе **Running Applications** появится активное приложение.

### 6. Остановить кластер

```bash
docker compose down
```

---

## Датасет

**Video Game Sales** — данные о продажах видеоигр с тиражом более 100 000 копий.

Источник: [Kaggle — Video Game Sales](https://www.kaggle.com/datasets/gregorut/videogamesales)

| Поле | Описание |
|---|---|
| Rank | Место в рейтинге по мировым продажам |
| Name | Название игры |
| Platform | Платформа (PS2, Wii, X360 и др.) |
| Year | Год выпуска |
| Genre | Жанр |
| Publisher | Издатель |
| NA_Sales | Продажи в Северной Америке (млн копий) |
| EU_Sales | Продажи в Европе (млн копий) |
| JP_Sales | Продажи в Японии (млн копий) |
| Other_Sales | Продажи в других регионах (млн копий) |
| Global_Sales | Суммарные мировые продажи (млн копий) |

---

## Пайплайн обработки данных

Все шаги реализованы в `notebooks/01_pyspark_data_processing.ipynb`:

1. **Загрузка CSV** — чтение `vgsales.csv` с автоматическим выводом схемы
2. **Очистка и нормализация** — удаление пустых значений, приведение типов
3. **Суммарные продажи по годам** — агрегация глобальных продаж в разрезе года выпуска
4. **Продажи по году, платформе и региону** — сгруппированные агрегации по NA, EU, JP, Other
5. **Доля продаж через Window Functions** — расчёт доли каждой платформы внутри года с использованием `Window.partitionBy`
6. **Доля продаж по издателю** — ранжирование издателей по доле в суммарных мировых продажах
7. **Суммарные продажи по игре** — суммирование региональных столбцов в единое поле `total_sales`
8. **Сохранение в Parquet** — запись итогового датафрейма в `data/processed/videogame_sales_parquet/`

---

## Скриншоты

### Spark Cluster UI — 3 worker-узла активны
![Spark Cluster UI](screenshots/spark_cluster_ui.png)

### Запущенные контейнеры Docker
![Docker Containers](screenshots/docker_containers_running.png)

### Подключение SparkSession к кластеру
![SparkSession](screenshots/spark_session_connection.png)

### Предпросмотр датасета
![Dataset Preview](screenshots/dataset_preview.png)

### Результат в формате Parquet
![Parquet Result](screenshots/parquet_result.png)

---

## Результаты

- Итоговый датасет сохранён в формате Parquet: `data/processed/videogame_sales_parquet/`
- Количество строк до сохранения совпадает с количеством строк после чтения из Parquet
- Все трансформации выполнены через PySpark DataFrame API на распределённом Spark-кластере

---

## Примечания

- Датасет `vgsales.csv` включён в репозиторий: `data/raw/`
- Обработанные данные в формате Parquet включены в репозиторий: `data/processed/`
- JupyterLab работает без токена и пароля — доступен по адресу [http://localhost:8888](http://localhost:8888)
