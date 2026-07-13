# Custom Airflow image with the project's pipeline dependencies
# (pandas, pyarrow, sqlalchemy, psycopg2, etc.) baked in, and the
# project code mounted/copied in alongside the DAG.
FROM apache/airflow:2.9.3-python3.11

USER root

# Nothing extra needed at the OS level for this project currently,
# kept here in case future dependencies need system libraries.

USER airflow

COPY requirements.txt /requirements.txt

# Airflow itself is already installed in the base image; only
# install the project's OTHER dependencies on top of it.
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    pyarrow \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    lxml \
    openpyxl
