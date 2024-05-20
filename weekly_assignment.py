from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import pandas as pd
from datetime import datetime
import ast
import json

# DAG
dag = DAG(
    'weekly_assignment',
    schedule_interval='@once',
    start_date=datetime(2024, 5, 16),
)

#flatten json like data on the lists
def flatten_products(row):
    products = row['products']
    
    # Check if products is a string, then parse it
    if isinstance(products, str):
        # Convert single quotes to double quotes to form a valid JSON string
        products = json.loads(products.replace("'", '"'))


    flat_data = []
    for product in products:
        flat_product = {
            'cart_id': row['id'],
            'cart_total': row['total'],
            'cart_discounted_total': row['discountedTotal'],
            'user_id': row['userId'],
            'total_products': row['totalProducts'],
            'total_quantity': row['totalQuantity'],
            'product_id': product['id'],
            'product_title': product['title'],
            'product_price': product['price'],
            'product_quantity': product['quantity'],
            'product_total': product['total'],
            'product_discount_percentage': product['discountPercentage'],
            'product_discounted_price': product['discountedPrice'],
            'product_thumbnail': product['thumbnail']
        }
        flat_data.append(flat_product)
    return flat_data


def transform_users(**kwargs):
    users = kwargs['ti'].xcom_pull(task_ids='extract_users')
    df = pd.DataFrame(users)
    df.to_csv('/opt/airflow/dags/playground/target/weekly/users.csv', index=False)


def transform_carts(**kwargs):
    carts = kwargs['ti'].xcom_pull(task_ids='extract_carts')
    df = pd.DataFrame(carts)

    #new flattened
    flat_data = []
    for index, row in df.iterrows():
        flat_data.extend(flatten_products(row))

    # Create a new DataFrame from the flattened data
    flat_df = pd.DataFrame(flat_data)

    # Save the flattened DataFrame to a new CSV file
    flat_df.to_csv('/opt/airflow/dags/playground/target/weekly/carts.csv', index=False)



def transform_posts(**kwargs):
    posts = kwargs['ti'].xcom_pull(task_ids='extract_posts')
    df = pd.DataFrame(posts)
    df.to_csv('/opt/airflow/dags/playground/target/weekly/posts.csv', index=False)

def transform_todos(**kwargs):
    todos = kwargs['ti'].xcom_pull(task_ids='extract_todos')
    df = pd.DataFrame(todos)
    df.to_csv('/opt/airflow/dags/playground/target/weekly/todos.csv', index=False)

# Check if the API is available
api_available = HttpSensor(
    task_id='api_available',
    http_conn_id='dummy_user',
    endpoint='users',
    request_params={},
    response_check=lambda response: response.status_code == 200,
    poke_interval=5,
    timeout=20,
    dag=dag,
)

# Extract tasks
extract_users = SimpleHttpOperator(
    task_id='extract_users',
    http_conn_id='dummy_user',
    endpoint='users',
    method='GET',
    response_filter=lambda response: response.json()['users'],
    log_response=True,
    dag=dag,
)

extract_carts = SimpleHttpOperator(
    task_id='extract_carts',
    http_conn_id='dummy_user',
    endpoint='carts',
    method='GET',
    response_filter=lambda response: response.json()['carts'],
    log_response=True,
    dag=dag,
)

extract_posts = SimpleHttpOperator(
    task_id='extract_posts',
    http_conn_id='dummy_user',
    endpoint='posts',
    method='GET',
    response_filter=lambda response: response.json()['posts'],
    log_response=True,
    dag=dag,
)

extract_todos = SimpleHttpOperator(
    task_id='extract_todos',
    http_conn_id='dummy_user',
    endpoint='todos',
    method='GET',
    response_filter=lambda response: response.json()['todos'],
    log_response=True,
    dag=dag,
)

# Transform tasks
transform_users = PythonOperator(
    task_id='transform_users',
    python_callable=transform_users,
    provide_context=True,
    dag=dag,
)

transform_carts = PythonOperator(
    task_id='transform_carts',
    python_callable=transform_carts,
    provide_context=True,
    dag=dag,
)

transform_posts = PythonOperator(
    task_id='transform_posts',
    python_callable=transform_posts,
    provide_context=True,
    dag=dag,
)

transform_todos = PythonOperator(
    task_id='transform_todos',
    python_callable=transform_todos,
    provide_context=True,
    dag=dag,
)


# Upload to GCS tasks
users_to_gcs = LocalFilesystemToGCSOperator(
    task_id='users_to_gcs',
    src='/opt/airflow/dags/playground/target/weekly/users.csv',
    dst='users/users.csv',
    bucket='weekly_6',
    dag=dag,
)

carts_to_gcs = LocalFilesystemToGCSOperator(
    task_id='carts_to_gcs',
    src='/opt/airflow/dags/playground/target/weekly/carts.csv',
    dst='carts/carts.csv',
    bucket='weekly_6',
    dag=dag,
)

posts_to_gcs = LocalFilesystemToGCSOperator(
    task_id='posts_to_gcs',
    src='/opt/airflow/dags/playground/target/weekly/posts.csv',
    dst='posts/posts.csv',
    bucket='weekly_6',
    dag=dag,
)

todos_to_gcs = LocalFilesystemToGCSOperator(
    task_id='todos_to_gcs',
    src='/opt/airflow/dags/playground/target/weekly/todos.csv',
    dst='todos/todos.csv',
    bucket='weekly_6',
    dag=dag,
)

# Define the BigQuery load tasks
load_users_to_bigquery = GCSToBigQueryOperator(
    task_id='load_users_to_bigquery',
    bucket='weekly_6',
    source_objects=['users/users.csv'],
    destination_project_dataset_table='stately-node-363801.weekly_6.users',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

load_carts_to_bigquery = GCSToBigQueryOperator(
    task_id='load_carts_to_bigquery',
    bucket='weekly_6',
    source_objects=['carts/carts.csv'],
    destination_project_dataset_table='stately-node-363801.weekly_6.carts',
    source_format='CSV', 
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

load_posts_to_bigquery = GCSToBigQueryOperator(
    task_id='load_posts_to_bigquery',
    bucket='weekly_6',
    source_objects=['posts/posts.csv'],
    destination_project_dataset_table='stately-node-363801.weekly_6.posts',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

load_todos_to_bigquery = GCSToBigQueryOperator(
    task_id='load_todos_to_bigquery',
    bucket='weekly_6',
    source_objects=['todos/todos.csv'],
    destination_project_dataset_table='stately-node-363801.weekly_6.todos',
    source_format='CSV',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)



#ADDITIONAL, summary 
sql_query = """
    SELECT 
        * 
    FROM `stately-node-363801.weekly_6.users` 
    WHERE age >= 21
    """
user_age_table = BigQueryInsertJobOperator(
        task_id='user_age_table',
        configuration={
            "query": {
                "query": sql_query,
                "destinationTable": {
                    "projectId": "stately-node-363801",
                    "datasetId": "weekly_6",
                    "tableId": "user_age_adult_summary"
                },
                "writeDisposition": "WRITE_TRUNCATE",  # Overwrites the table if it already exists
                "useLegacySql": False  # Use Standard SQL syntax
            }
        }
    )



# Set task dependencies
api_available >> [extract_users, extract_carts, extract_posts, extract_todos]
extract_users >> transform_users >> users_to_gcs >> load_users_to_bigquery 
extract_carts >> transform_carts >> carts_to_gcs >> load_carts_to_bigquery
extract_posts >> transform_posts >> posts_to_gcs >> load_posts_to_bigquery
extract_todos >> transform_todos >> todos_to_gcs >> load_todos_to_bigquery

load_users_to_bigquery >> user_age_table

# extract_users >> transform_users >> users_to_gcs 
# extract_carts >> transform_carts >> carts_to_gcs 
# extract_posts >> transform_posts >> posts_to_gcs 
# extract_todos >> transform_todos >> todos_to_gcs