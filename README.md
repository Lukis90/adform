# Adform task

Incase you don't have docker installed you can find instructions on how to do it
[here](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository).
For java you can either run `sudo apt install openjdk-21-jre-headless` or research
some other installation method :smiley:.

An ETL for calculating clicks and impressions counts from parquet files in
raw_data folder. After ETL is done, output folder is populated with a csv file
in form:

| Date       | hour | clicks_count | impressions_count |
| :--------- | :--: | :----------: | :---------------: |
| 2022-05-27 |   9  |      0       |         0         |
| 2022-05-27 |  10  |      0       |         0         |
| 2022-05-27 |  11  |     10       |         0         |
| 2022-05-27 |  12  |     20       |        10         |

Same results are also sent to local postgresql instance, `adform` database,
`adds_success_history` table in form:

| datetime         | clicks_count | impressions_count | audit_loaded_datetime   |
| :--------------- | :----------: | :---------------: | :---------------------- |
| 2022-05-27 09:00 |      0       |         0         | 2024-08-29 13:44:53.123 |
| 2022-05-27 10:00 |      0       |         0         | 2024-08-29 13:44:53.123 |
| 2022-05-27 11:00 |     10       |         0         | 2024-08-29 13:44:53.123 |
| 2022-05-27 12:00 |     20       |        10         | 2024-08-29 13:44:53.123 |

To run this ETL for the first time, there few things that needs to be done.

1. First you will need to start database. To do this run this command:

        docker compose up -d

    command from inside parent directory (same that contains README.md file). This will start
    postgres database and will create necessary table. You can find root password in `sec`
    directory `db_root_password.txt` file. You should see `Container adform_postgres Started`
    output at the end incase of success. 

2. You will need to install python dependencies (again from parent dir):

        pip install -r requirements/prod.txt

    To execute pytests you will need to install `dev` dependencies instead (it will take some time):

        pip install -r requirements/dev.txt

3. You will need to download [postgresql-42.7.4.jar](https://jdbc.postgresql.org/download/)
    for java 1.8+. If you have an older java version, you will find the instructions for that
    on the same page. After you've downloaded postgres jdbc driver you will need to update
    .env file `JDBC_DRIVER_PATH` variable with the path to your downloaded jar.

4. You can now run ETL. Execute this from `src` directory:

        python main.py

To launch pytests, first make sure you correctly installed dependencies from step 2, then
simply run this from parent directory:

        pytest
