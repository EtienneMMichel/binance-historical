import requests, zipfile, io
import shutil
import requests
import xmltodict
from datetime import datetime
from .. import exceptions
import os

import pandas as pd

S3_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix={path}"

FETCHING_URL = "https://data.binance.vision/{file_path}"


def download_zip(url, saving_dir):
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(saving_dir)

def delete_temp_data(path):
    shutil.rmtree(path)



def get_data(path, next_marker=None, rotation=False):
    url = S3_URL.format(path=path)
    url = url if next_marker is None else url + f"&marker={next_marker}"
    if rotation:
        print("ROTATION")
        from dotenv import load_dotenv
        from requests_ip_rotator import ApiGateway, EXTRA_REGIONS
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        gateway = ApiGateway(url, access_key_id = AWS_ACCESS_KEY_ID, access_key_secret = AWS_SECRET_ACCESS_KEY)
        gateway.start()
        s = requests.Session()
        s.mount(url, gateway)
        r = s.get(url)
    else:
        r = requests.get(url)
    return xmltodict.parse(r.text)

def get_date(zip_file_name):
    if len(zip_file_name) > 4:
        date_list = zip_file_name[-3:]
        date = datetime(year=int(date_list[0]), month=int(date_list[1]), day=int(date_list[2]))
    else:
        date_list = zip_file_name[-2:]
        date = datetime(year=int(date_list[0]), month=int(date_list[1]), day=1)
    return date


def extract_data(db, data, path, start_date, end_date, data_path, is_local=False):
    contents = data.get("ListBucketResult", {"Contents":None}).get("Contents", None)
    if contents is None:
        return
    for content in contents:
        if not "Key" in list(content.keys()):
            raise exceptions.FailedToExtractException(content)
        if content["Key"][-4:] == ".zip" and not os.path.exists(f"SAVING_DIR/{content['Key']}"):
            saving_dir = data_path + "/" + "/".join(content["Key"].split("/")[-3:-1])
            zip_file_name = content["Key"].split("/")[-1].split(".")[0].split("-")
            date = get_date(zip_file_name)
            if not is_local and db.already_in_table(path, date):
                continue
            if start_date <= date and date < end_date: 
                os.makedirs(saving_dir, exist_ok=True)
                download_zip(url=FETCHING_URL.format(file_path=content["Key"]), saving_dir=saving_dir)

def get_binance_data(path, db, start_date, end_date, data_path, is_local=False, rotation=False):
    next_marker = None
    data = get_data(path,rotation=rotation)
    extract_data(db, data, path, start_date, end_date, data_path, is_local)
    while 'NextMarker' in list(data.get("ListBucketResult", {}).keys()):
        next_marker = data["ListBucketResult"]['NextMarker']
        data = get_data(path, next_marker=next_marker, rotation=rotation)
        extract_data(db, data, path, start_date, end_date, data_path, is_local)


def get_data_to_save(path, extract_binance_historical_data):
    res = None
    for filename in os.listdir(path):
        df = pd.read_csv(f"{path}/{filename}", header=None)
        df = extract_binance_historical_data(df)
        res = df.copy(deep=True) if res is None else pd.concat([res, df], axis=0)
    
    return res

