# MSSql Server database migration to Snowflake using Airflow


ERD flowchart (one-to-many relationships between the tables, two bridge tables: film_category and film_actor)
![erd_schema](https://github.com/indrajos/MSSqlServer_Airflow_Snowflake_pipeline/assets/94143462/76c3e7c2-f30d-47d4-aed9-f28dd13a758d)



## Run commands inside the project folder

Prerequisites: docker installed on a test machine

Initial airflow db setup:

```
docker compose up airflow-init
```

Start all airflow containers:

```
docker compose up
```


The web server is available at http://localhost:8080. 
The default account has the login airflow and the password airflow.

Connect to the Docker container as root user (if needed):

```
docker exec -u root -ti intus-airflow-worker-1 bash
```


Closing:

```
docker compose down
```


### Reports can be found in Logs of Airflow:

![af_logs_reports](https://github.com/indrajos/MSSqlServer_Airflow_Snowflake_pipeline/assets/94143462/e61fed3f-7957-4058-a60f-5bfacb47ca43)


### The result - the database from MSSql Server after testing migrated to Snowflake DB using Airflow and Docker:

![sf_db](https://github.com/indrajos/MSSqlServer_Airflow_Snowflake_pipeline/assets/94143462/54f2fa2f-d8a9-4f21-ae49-370842b713ca)


![sf_table_preview](https://github.com/indrajos/MSSqlServer_Airflow_Snowflake_pipeline/assets/94143462/65b78257-59a9-4861-b7fe-2aa4442680a0)

