from google.cloud import bigquery
from tabulate import tabulate
from datetime import date,timedelta

import pandas as pd
import json

def analyze_tables(project_id1, dataset_id1, table_name1, 
                   project_id2, dataset_id2, table_name2, 
                   pk,
                   fields,
                   project_id = "default_project",
                   partition_date1 = date.today()-timedelta(days=1),
                   partition_date2 = date.today()-timedelta(days=1)):
    """

    Args:

    Returns:

    """

    print("START")
    client = bigquery.Client(project_id)

    print("The job will run on the project: " + project_id)

    intro_st = tabulate([[project_id1, dataset_id1, table_name1, partition_date1], 
                    [project_id2, dataset_id2, table_name2, partition_date2]], 
                   headers=['project', 'dataset', 'table', 'partition_date'])
    print(intro_st)
    
    list_df = []
    
    fields_str = ""
    for field in fields:
        fields_str = fields_str + field + ","

    tot = 0

    query = (
f"""
with field_count as (
SELECT 
{pk},
{fields_str}
FROM
  `{project_1}.{dataset_1}.{table_name1}`
WHERE
  DATE(partition_date) = DATE('{partition_date1}')
EXCEPT DISTINCT
SELECT 
{pk},
{fields_str}
FROM
  `{project_2}.{dataset_2}.{table_name2}`
WHERE
  DATE(partition_date) = DATE('{partition_date2}')
)
select count(*) as count_rows from field_count
"""
    )
    query_job = client.query(
        query,
        # Location must match that of the dataset(s) referenced in the query.
        location="EU",
    )  # API request - starts the query

    df = query_job.result().to_dataframe()
    df.index = ["ALL_FIELDS"]
    df['per_cent'] = 100
    tot = df.loc["ALL_FIELDS"]["count_rows"]
    list_df.append(df)

    for field in fields:
        print(field)
        query = (
f"""
with field_count as (
SELECT 
{pk},
{field},
FROM
  `{project_1}.{dataset_1}.{table_name1}`
WHERE
  DATE(partition_date) = DATE('{partition_date1}')
EXCEPT DISTINCT
SELECT 
{pk},
{field},
FROM
  `{project_2}.{dataset_2}.{table_name2}`
WHERE
  DATE(partition_date) = DATE('{partition_date2}')
)
select count(*) as count_rows from field_count
"""
        )
        query_job = client.query(
            query,
            # Location must match that of the dataset(s) referenced in the query.
            location="EU",
        )  # API request - starts the query

        df = query_job.result().to_dataframe()
        df.index = [field]
        value = df.loc[field]["count_rows"]
        df['per_cent'] = float("{:.2f}".format((value/tot)*100))
        list_df.append(df)
    
    final_df = pd.concat(df for df in list_df)
    
    print(final_df.to_string())

    file_name = "analysis_" + table_name1 + "_" + partition_date1 + "_v2.txt"
    f = open(file_name, "a")
    f.write(intro_st + "\n")
    f.write(final_df.to_string()+"\n")
    f.close()

    print("\nDONE")

if __name__ == '__main__':
    f = open('data.json')

    data = json.load(f)

    project_1 = data[0]['project']
    dataset_1 = data[0]['dataset']
    table_1 = data[0]['table']
    pk_1 = data[0]['PK']
    fields_1 = data[0]['fields']
    partition_date_1 = data[0]['partition_date']

    project_2 = data[1]['project']
    dataset_2 = data[1]['dataset']
    table_2 = data[1]['table']
    pk_2 = data[1]['PK']
    fields_2 = data[0]['fields']
    partition_date_2 = data[1]['partition_date']

    print(project_1 + ":" + dataset_1 + "." + table_1)
    print(project_2 + ":" + dataset_2 + "." + table_2 + "\n")

    f.close()

    analyze_tables(project_1, dataset_1, table_1, 
                   project_2, dataset_2, table_2, 
                   pk_1,
                   fields_1,
                   partition_date1=partition_date_1,
                   partition_date2=partition_date_2)