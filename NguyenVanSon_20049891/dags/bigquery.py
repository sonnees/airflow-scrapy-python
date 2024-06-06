import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./son.json"
from google.cloud import bigquery

table_id = 'stellar-works-416715.son.son1'

def a():
    try :
        client = bigquery.Client()

        query = f"""
            SELECT
                COUNT(*) AS Total_People_With_Heart_Disease
            FROM
               {table_id}
            WHERE
                target = '1'
        """
        query_job = client.query(query)
        rows = query_job.result()

        for row in rows:
            print(f"Số lượng người mắc bệnh tim: {row.Total_People_With_Heart_Disease}")
        
    except Exception as e:
        print(f'Error: {e}')

def b():
    try:
        client = bigquery.Client()
        query = f"""
            SELECT
                COUNT(*) AS Total_Count
            FROM
                {table_id}
            WHERE
                exercise_induced_angina = 'yes'
        """
        query_job = client.query(query)
        rows = query_job.result()
        for row in rows:
            print(f"Số lượng người có đau ngực do tập thể dục: {row.Total_Count}")

    except Exception as e:
        print(f'Error: {e}')

b()
# a()
