from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.models.taskinstance import TaskInstance as ti

def _transform(ti: ti):   
   import requests
   resp = requests.get(f'https://swapi.dev/api/people/1').json()
   print(resp)
   my_character = {}
   my_character["height"] = int(resp["height"]) - 20
   my_character["mass"] = int(resp["mass"]) - 13
   my_character["hair_color"] = "black" if resp["hair_color"] == "blond" else "blond"
   my_character["eye_color"] = "hazel" if resp["eye_color"] == "blue" else "blue"
   my_character["gender"] = "female" if resp["gender"] == "male" else "female"
   ti.xcom_push("character_info", my_character)


def _transform2(ti: ti):
   import requests
   resp = requests.get(f'https://swapi.dev/api/people/2').json()
   print(resp)
   my_character = {}
   my_character["height"] = int(resp["height"]) - 50
   my_character["mass"] = int(resp["mass"]) - 20
   my_character["hair_color"] = "burgundy" if resp["hair_color"] == "blond" else "brown"
   my_character["eye_color"] = "green" if resp["eye_color"] == "blue" else "black"
   my_character["gender"] = "male" if resp["gender"] == "male" else "female"
   ti.xcom_push("character_info", my_character)


def _load(values):
   print(values)


with DAG(
   'xcoms_templating',
   schedule = None,
   start_date = pendulum.datetime(2023,3,1),
   catchup = False
): 

   t1 = PythonOperator(
       task_id = '_transform',
       python_callable = _transform
   )

   t2 = PythonOperator(
       task_id = 'load',
       python_callable = _load,
       op_args = ["{{ ti.xcom_pull(task_ids=['_transform','_transform2'], key='character_info') }}"]
   )

   t3 = PythonOperator(
       task_id = '_transform2',
       python_callable = _transform2,
   )

   [t1,t3] >> t2