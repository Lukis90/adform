# Adform task

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

1. You will need to setup database. To do this you can run:

        bash db_first_run.sh

    command from inside parent directory (same that contains README.md file). This will start 
    postgres database and will create necessary table. It will ask for root db password which
    can be found in `sec` directory `db_root_password.txt` file. You should see `CREATE TABLE`
    output at the end incase of success. 

2. You will need to install python dependencies (again from parent dir):

        pip install -r requirements/prod.txt

    To execute pytests you will need to install `dev` dependencies instead:

        pip install -r requirements/dev.txt


3. You can now run ETL. Execute this from src directory:

        python main.py
