FROM apache/airflow:2.8.3
RUN pip install apache-airflow-providers-microsoft-azure==1.2.0rc1
RUN pip install apache-airflow-providers-microsoft-mssql
