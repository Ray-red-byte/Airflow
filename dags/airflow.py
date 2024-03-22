from __future__ import annotations

import pendulum
import os
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



from google.cloud import storage
import logging
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.bash import BashOperator

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import json
from google.cloud import storage
from airflow.models import XCom



# field_configure
schema_field = {"electric_month": [
                {'name': 'date_year_month', 'type': 'DATE', 'mode': 'NULLABLE'},
                {'name': 'unit', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_national', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_pumped_storage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_thermal_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_thermal_coal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_thermal_oil', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_thermal_gas', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_nuclear', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_hydro', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_geothermal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_solar', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_wind', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_biomass', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'total_power_generation_renewable_waste', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_pumped_storage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_thermal_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_thermal_coal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_thermal_oil', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_thermal_gas', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_nuclear', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_renewable_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_renewable_hydro', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_renewable_geothermal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_renewable_solar', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'tepco_power_generation_renewable_wind', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_thermal_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_thermal_coal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_thermal_gas', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_renewable_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_renewable_hydro', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_renewable_geothermal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_renewable_solar', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'private_power_plant_generation_renewable_wind', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_thermal_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_thermal_coal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_thermal_oil', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_thermal_gas', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_total', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_hydro', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_geothermal', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_solar', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_wind', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_biomass', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'self_use_power_generation_renewable_waste', 'type': 'FLOAT', 'mode': 'NULLABLE'}
            ]
        }

# function to parse JSON
def parse_electric_month(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

        english_names = {
            "日期(年/月)": "date_year_month",
            "單位": "unit",
            "全國發電量_總計": "total_power_generation_national",
            "全國發電量_抽蓄水力": "total_power_generation_pumped_storage",
            "全國發電量_火力_合計": "total_power_generation_thermal_total",
            "全國發電量_火力_燃煤": "total_power_generation_thermal_coal",
            "全國發電量_火力_燃油": "total_power_generation_thermal_oil",
            "全國發電量_火力_燃氣": "total_power_generation_thermal_gas",
            "全國發電量_核能": "total_power_generation_nuclear",
            "全國發電量_再生能源_合計": "total_power_generation_renewable_total",
            "全國發電量_再生能源_慣常水力": "total_power_generation_renewable_hydro",
            "全國發電量_再生能源_地熱": "total_power_generation_renewable_geothermal",
            "全國發電量_再生能源_太陽光電": "total_power_generation_renewable_solar",
            "全國發電量_再生能源_風力": "total_power_generation_renewable_wind",
            "全國發電量_再生能源_生質能": "total_power_generation_renewable_biomass",
            "全國發電量_再生能源_廢棄物": "total_power_generation_renewable_waste",
            "台電發電量_合計": "tepco_power_generation_total",
            "台電發電量_抽蓄水力": "tepco_power_generation_pumped_storage",
            "台電發電量_火力_小計": "tepco_power_generation_thermal_total",
            "台電發電量_火力_燃煤": "tepco_power_generation_thermal_coal",
            "台電發電量_火力_燃油": "tepco_power_generation_thermal_oil",
            "台電發電量_火力_燃氣": "tepco_power_generation_thermal_gas",
            "台電發電量_核能": "tepco_power_generation_nuclear",
            "台電發電量_再生能源_小計": "tepco_power_generation_renewable_total",
            "台電發電量_再生能源_慣常慣常水力": "tepco_power_generation_renewable_hydro",
            "台電發電量_再生能源_地熱": "tepco_power_generation_renewable_geothermal",
            "台電發電量_再生能源_太陽光電": "tepco_power_generation_renewable_solar",
            "台電發電量_再生能源_風力": "tepco_power_generation_renewable_wind",
            "民營電廠發電量_合計": "private_power_plant_generation_total",
            "民營電廠發電量_火力_小計": "private_power_plant_generation_thermal_total",
            "民營電廠發電量_火力_燃煤": "private_power_plant_generation_thermal_coal",
            "民營電廠發電量_火力_燃氣": "private_power_plant_generation_thermal_gas",
            "民營電廠發電量_再生能源_小計": "private_power_plant_generation_renewable_total",
            "民營電廠發電量_再生能源_慣常水力": "private_power_plant_generation_renewable_hydro",
            "民營電廠發電量_再生能源_地熱": "private_power_plant_generation_renewable_geothermal",
            "民營電廠發電量_再生能源_太陽光電": "private_power_plant_generation_renewable_solar",
            "民營電廠發電量_再生能源_風力": "private_power_plant_generation_renewable_wind",
            "自用發電設備發電量_合計": "self_use_power_generation_total",
            "自用發電設備發電量_火力_小計": "self_use_power_generation_thermal_total",
            "自用發電設備發電量_火力_燃煤": "self_use_power_generation_thermal_coal",
            "自用發電設備發電量_火力_燃油": "self_use_power_generation_thermal_oil",
            "自用發電設備發電量_火力_燃氣": "self_use_power_generation_thermal_gas",
            "自用發電設備發電量_再生能源_小計": "self_use_power_generation_renewable_total",
            "自用發電設備發電量_再生能源_慣常水力": "self_use_power_generation_renewable_hydro",
            "自用發電設備發電量_再生能源_地熱": "self_use_power_generation_renewable_geothermal",
            "自用發電設備發電量_再生能源_太陽光電": "self_use_power_generation_renewable_solar",
            "自用發電設備發電量_再生能源_風力": "self_use_power_generation_renewable_wind",
            "自用發電設備發電量_再生能源_生質能": "self_use_power_generation_renewable_biomass",
            "自用發電設備發電量_再生能源_廢棄物": "self_use_power_generation_renewable_waste"
        }

        for item in data:
            for chinese_name, english_name in english_names.items():
                if chinese_name in item:
                    if chinese_name == "日期(年/月)":
                        temp = item.pop(chinese_name) + '01'
                        date = datetime.strptime(temp, '%Y%m%d')
                        item[english_name] = date.strftime('%Y-%m-%d')
                    else:    
                        item[english_name] = item.pop(chinese_name)

        load_bigquery_electric_month_data = data

    temp_file_path = "./dags/data/temp_file.json"
    with open(temp_file_path, 'w') as temp_file:
        # Dump data to the temporary file in JSON format
        json.dump(load_bigquery_electric_month_data, temp_file, ensure_ascii=False)


def parse_electricity_storage(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        
        load_bigquery_electric_data = data

    return load_bigquery_electric_data


def parse_particle(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        
        load_bigquery_particle_data = data["records"]
    
    return load_bigquery_particle_data


def parse_rain(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        
        each_station_rain_data = data["cwaopendata"]["dataset"]["Station"]
        for item in each_station_rain_data:
            item["StationName"]
            item["StationId"]
            item["ObsTime"]["DateTime"]
            item["RainfallElement"]
    
    return load_bigquery_rain_data        


def parse_sand(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        load_bigquery_sand_data = data["records"]
    
    return load_bigquery_sand_data


def insert_from_temp_file(temp_file_path, table_name):

    
    with open(temp_file_path, 'r') as temp_file:
        data = json.load(temp_file)
    
    from google.cloud import bigquery

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './dags/data/sound-silicon-417606-f3628e8dc20e.json'

    
    client = bigquery.Client()
    table_ref = client.dataset('Appworks_ray').table(table_name)
    
    # Assuming 'data' is a list of dictionaries representing rows to insert
    job = client.insert_rows_json(table_ref, data)

    if job:
        print('Encountered errors while inserting data:', job)
    else:
        print('Data inserted successfully.')
    
    os.remove(temp_file_path)

# GCS setting
func_list = [parse_electric_month, parse_electricity_storage, parse_particle, parse_rain, parse_sand]
gcs_files = ['electric_month', 'electricity_storage', 'particle', 'rain', 'sand']


with DAG(
    dag_id='public_data_to_bigquery',
    description='Fetch data from public URL and load into BigQuery',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule='@daily',
) as dag:


    for index, filename in enumerate(gcs_files):
        parse_gcs_data = GCSToLocalFilesystemOperator(
            task_id=f'parse_gcs_data_{filename}',
            bucket='airflow_ray',
            object_name= filename + '.json',
            gcp_conn_id='google_cloud_conn',
            filename=f'./dags/data/{filename}.json',
            dag=dag,
        )

        parse_file = PythonOperator(
            task_id=f'parse_file_{filename}',
            python_callable=func_list[index],
            op_kwargs={'file_path': f'./dags/data/{filename}.json', 'task_id': f'parse_file_{filename}'},
            provide_context=True,
            dag=dag,
        )

        
        create_bigquery_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_bigquery_{filename}_table',
            dataset_id='Appworks_ray',     
            table_id=filename,                             
            schema_fields = schema_field[filename],
            gcp_conn_id='google_cloud_conn',
            dag=dag,
        )


        insert_into_bigquery = PythonOperator(
            task_id='insert_into_bigquery',
            python_callable=insert_from_temp_file,
            op_kwargs={'temp_file_path': './dags/data/temp_file.json', 'table_name': f"{filename}.json"},
            provide_context=True,  
            dag=dag
        )



        parse_gcs_data >>  parse_file >> create_bigquery_table >> insert_into_bigquery