import base64
import os
import requests
from google.cloud import bigquery


class Config:
    dataset_id = os.environ.get("dataset_id")
    table_th = os.environ.get("table_th")
    url_th = os.environ.get("url_th")
    table_bkk = os.environ.get("table_bkk")
    url_bkk = os.environ.get("url_bkk")


def get_data(url: str) -> dict:
    response = requests.get(url)
    return response.json()


def insert_data(event, context):
    client = bigquery.Client()
    dataset_ref = client.dataset(Config.dataset_id)

    #covid19 report of thailand
    raw_th = get_data(Config.url_th)
    th = raw_th[0]
    record_th = [(
        th["update_date"], 
        th["new_case"], 
        th["total_case"],
        th["new_case_excludeabroad"],
        th["total_case_excludeabroad"],
        th["new_death"],
        th["total_death"],
        th["new_recovered"],
        th["total_recovered"],
        )]

    table_th_ref = dataset_ref.table(Config.table_th)
    table_th = client.get_table(table_th_ref)
    result_th = client.insert_rows(table_th, record_th)

    #covid19 report of bangkok
    raw_bkk = get_data(Config.url_bkk)
    bkk = raw_bkk[1]
    record_bkk = [(
        bkk["update_date"], 
        bkk["new_case"], 
        bkk["total_case"],
        bkk["new_case_excludeabroad"],
        bkk["total_case_excludeabroad"],
        bkk["new_death"],
        bkk["total_death"],
        )]
        
    table_bkk_ref = dataset_ref.table(Config.table_bkk)
    table_bkk = client.get_table(table_bkk_ref)
    result_bkk = client.insert_rows(table_bkk, record_bkk)

    return result_th,result_bkk