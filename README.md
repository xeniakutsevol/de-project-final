# Проектная работа

### Описание
Реализован пайплайн обработки данных из нескольких источников и хранилище данных, пайплайн формирования витрины в соответствии с запросом бизнеса, BI-аналитика для компании. DWH-хранилище реализовано на базе аналитической БД **Vertica**. ETL-процесс автоматизирован через выгрузку данных из **S3-хранилища** в DWH с помощью **DAG Airflow**. Построение витрины также автоматизировано через Airflow. Метрики компании отражены в дашборде на базе **Metabase**.

### Структура репозитория
- `dags/` содержит DAGs Airflow для слоев Staging, DWH.
- `sql/` содержит DDL-скрипты.
- `img/` содержит скриншот дашборда из Metabase.

### Как работать с репозиторием
Задайте параметры подключения к S3-хранилищу и БД Vertica в файле `config.json` в папке `dags/`.