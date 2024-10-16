from airflow.decorators import task, dag
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

@dag(
    description="DAG to stress test",
    schedule=None,
    catchup=False,
)
def airflow_stress_test ():
    @task
    def load_file():
        sql_hook = MsSqlHook(mssql_conn_id='mssql_default')

        try:
            smb_conn_id = 'fs1_fin_data_store'
            hook = SambaHook(smb_conn_id)
            files = hook.listdir('.')
            print("Files in the given directory:")
            for f in files:
                print(f)
            file = hook.open_file("bulktest/airflow_stress_file.csv")
            
            #load data into data frame
            print(f"loading dataframe {rows} rows test, duration:", end='', flush=True)
            start_time = time.time()
            df = pd.read_csv(file, nrows=10, dtype='str')
            print("--- %s seconds ---" % (time.time() - start_time),flush=True)
            df = df.fillna("0")
            print(df.head())
            data = list(df.itertuples(index=False, name=None))
        except Exception as e:
            print(e)
        else:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()

            print(f"vanilla insert {rows} rows test, duration:", end='')
            
            query = """INSERT INTO dbo.AIRFLOW_STRESS_TEST_TARGET
                            ([INDEX]
                            ,SCHOOL_YEAR
                            ,DATA_LEVEL
                            ,PUBLIC_OR_INDEPENDENT
                            ,DISTRICT_NUMBER
                            ,DISTRICT_NAME
                            ,SCHOOL_NUMBER
                            ,SCHOOL_NAME
                            ,FACILITY_TYPE
                            ,GRADE
                            ,TOTAL_STUDENTS
                            ,INDIGENOUS_STUDENTS
                            ,NON_INDIGENOUS_STUDENTS
                            ,ELL_STUDENTS
                            ,NON_ELL_STUDENTS
                            ,FRENCH_IMMERSION_STUDENTS
                            ,NON_FRENCH_IMMERSION_STUDENTS
                            ,DIVERSE_ABILITIES_STUDENTS
                            ,NON_DIVERSE_ABILITIES_STUDENTS
                            ,RESIDENT_STUDENTS
                            ,NON_RESIDENT_STUDENTS
                            ,ADULT_STUDENTS)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """
            
            start_time = time.time()
            #cursor.executemany(query, data)

            print("--- %s seconds ---" % (time.time() - start_time),flush=True)

            #cursor.execute("TRUNCATE TABLE dbo.AIRFLOW_STRESS_TEST_TARGET")

    load_file()

airflow_stress_test()
