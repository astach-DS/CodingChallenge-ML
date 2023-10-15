from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    'challenge_dag',
    start_date=datetime(2023, 9, 26),
    schedule_interval=None
)
def taskflow():

    get_data = SimpleHttpOperator(
        task_id='get_data',
        method='GET',
        http_conn_id='ml',
        endpoint='sites/MLA/search?category=MLA1577#json',
        response_filter=lambda response: response.json()["results"],
        log_response=True
    )

    @task
    def filter_data(file_path, desired_keys):
        import json
        
        with open(f'{file_path}', 'r') as file:
            data = json.load(file)
        
        def filter_function(data, desired_keys: list[str]):
            list_of_values = [[str(d[key]) for key in desired_keys] for d in data]
            # tuple_of_values = [tuple(value,) for value in list_of_values]
            return list_of_values
        
        return filter_function(data, desired_keys)

    @task
    def insert_into_snowflake(table, list_of_values):
        placeholders = ', '.join(['%s'] * len(list_of_values[0]))
        query = f"""
        INSERT INTO {table}  ('id', 'site_id', 'title', 'price', 'sold_quantity', 'thumbnail')  
        VALUES ({placeholders})
        """

        snowflake_task = SnowflakeOperator(
            task_id='insert_into_snowflake',
            sql=query,
            parameters=list_of_values,
            snowflake_conn_id='snowflake',
            autocommit=True
        )
        snowflake_task.execute(context=None)
        

    @task
    def to_json(file_path, data):
        import json
        
        data = json.dumps(data)
        with open(f'{file_path}', 'w') as file:
            file.write(data)

    # snowflake_ddl = SnowflakeOperator(
    #     task_id = 'snowflake_ddl',
    #     sql = """
    #         create table if not exists test.test2(
    #             aa varchar(20)
    #         );
    #     """,
    #     snowflake_conn_id = 'snowflake'
    # )

    # do_activity = BashOperator(
    #                 task_id='do_activity',
    #                 # This is the Bash command to run
    #                 bash_command=f"echo '$PWD'",
    #             )

    # postgres_task = PostgresOperator(
    #     task_id= 'postgres_task',
    #     sql= 'select * from test',
    #     postgres_conn_id= 'postgres'
    # )
    # snowflake_ddl
    # do_activity
    data = get_data.output
    save_data = to_json('include/test.json', data)
    filteredData = filter_data('include/test.json',['id', 'site_id', 'title', 'price', 'sold_quantity', 'thumbnail'])
    # insert_into_snowflake('test', filteredData)
    
    
    data >> save_data >> filteredData >> insert_into_snowflake('test', filteredData)
    # to_json('include/test.json', get_data.output) >> filter_data('include/test.json',['id', 'site_id', 'title', 'price', 'sold_quantity', 'thumbnail']) >> insert_into_snowflake

taskflow()