# Custom Airflow image with the project's pipeline dependencies
# (pandas, pyarrow, sqlalchemy, psycopg2, pymysql, etc.) baked in,
# and the project code mounted/copied in alongside the DAG.
#
# Airflow 3.x - ships the new React-based UI (served by the
# api-server component instead of the old Flask "webserver").
FROM apache/airflow:3.3.0-python3.11

USER root

# Nothing extra needed at the OS level for this project currently,
# kept here in case future dependencies need system libraries.

USER airflow

COPY requirements.txt /requirements.txt

# Airflow itself is already installed in the base image; only
# install the project's OTHER dependencies (from requirements.txt)
# on top of it. apache-airflow-providers-fab keeps the classic
# `airflow users create` / RBAC login flow working on Airflow 3,
# which ships a different default auth manager.
RUN pip install --no-cache-dir -r /requirements.txt
