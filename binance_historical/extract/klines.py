import requests
import xmltodict
import os
from tqdm import tqdm
from .. import utils
from .. import exceptions
import pandas as pd
from datetime import datetime
import math
from dateutil.relativedelta import relativedelta
from sqlalchemy.exc import ProgrammingError

FETCHING_URL = "https://data.binance.vision/{file_path}"
SAVING_DIR = "./DATA/temp/data"
S3_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix={path}"

def _get_data(path, next_marker=None):
    url = S3_URL.format(path=path)
    url = url if next_marker is None else url + f"&marker={next_marker}"
    r = requests.get(url)
    return xmltodict.parse(r.text)

def _extract_binance_historical_data(raw_binance_df):
    df =  pd.DataFrame({
        "timestamp":raw_binance_df[list(raw_binance_df.columns)[6]],
        "open_price":raw_binance_df[list(raw_binance_df.columns)[1]],
        "close_price":raw_binance_df[list(raw_binance_df.columns)[4]],
        "volume":raw_binance_df[list(raw_binance_df.columns)[5]],
        "count":raw_binance_df[list(raw_binance_df.columns)[8]],
        "buy_volume":raw_binance_df[list(raw_binance_df.columns)[9]],
        "sell_volume":raw_binance_df[list(raw_binance_df.columns)[10]]
    })
    df['datetime'] =  df['datetime']*eval(f"1e-{len(str(df['datetime'].tolist()[0])) - 10}")
    df["timestamp"] = [math.ceil(d) for d in df['datetime'].tolist()]
    df['datetime'] =  pd.to_datetime(df["timestamp"],unit='s')
    return df


def _get_data_to_save(path):
    res = None
    for filename in os.listdir(path):
        df = pd.read_csv(f"{path}/{filename}", header=None)
        res = df if res is None else pd.concat([res, df], axis=0)
    res = _extract_binance_historical_data(res)
    return res


def _get_date(zip_file_name):
    if len(zip_file_name) > 4:
        date_list = zip_file_name[-3:]
        date = datetime(year=int(date_list[0]), month=int(date_list[1]), day=int(date_list[2]))
    else:
        date_list = zip_file_name[-2:]
        date = datetime(year=int(date_list[0]), month=int(date_list[1]), day=1)
    return date


def _extract_data(db, data, path, start_date, end_date, data_path, is_local=False):
    contents = data.get("ListBucketResult", {"Contents":None}).get("Contents", None)
    if contents is None:
        return
    for content in contents:
        if not "Key" in list(content.keys()):
            raise exceptions.FailedToExtractException(content)
        if content["Key"][-4:] == ".zip" and not os.path.exists(f"SAVING_DIR/{content['Key']}"):
            saving_dir = data_path + "/" + "/".join(content["Key"].split("/")[-3:-1])
            zip_file_name = content["Key"].split("/")[-1].split(".")[0].split("-")
            date = _get_date(zip_file_name)
            if not is_local and db.already_in_table(path, date):
                continue
            if start_date <= date and date < end_date: 
                os.makedirs(saving_dir, exist_ok=True)
                utils.download_zip(url=FETCHING_URL.format(file_path=content["Key"]), saving_dir=saving_dir)

def _get_binance_data(path, db, start_date, end_date, data_path, is_local=False):
    next_marker = None
    data = _get_data(path)
    _extract_data(db, data, path, start_date, end_date, data_path, is_local)
    while 'NextMarker' in list(data.get("ListBucketResult", {}).keys()):
        next_marker = data["ListBucketResult"]['NextMarker']
        data = _get_data(path, next_marker)
        _extract_data(db, data, path, start_date, end_date, data_path, is_local)
    
    
    
    
def _local_saved(timeframe, fetch_date, symbol, is_next_month, data_path):
    month = "0" + str(fetch_date.month) if fetch_date.month < 10 else str(fetch_date.month)
    day = "0" + str(fetch_date.day) if fetch_date.day < 10 else str(fetch_date.day)
    fetch_date_str = f"{fetch_date.year}-{month}"
    if not is_next_month: fetch_date_str += f"-{day}"
    return os.path.exists(f"{data_path}/{symbol}/{timeframe}/{symbol}-{timeframe}-{fetch_date_str}.csv")

def _cloud_saved(timeframe, fetch_date, next_fetch_date, symbol, is_local=True):
    if is_local:
        return False
    table_name = f"{symbol}_{timeframe}"
    db = utils.Database()
    fetch_date_str = fetch_date.strftime('%Y-%d-%m')
    next_fetch_date_str = next_fetch_date.strftime('%Y-%d-%m')
    try:
        count = db.count_elements_in_table(table_name, additional_query=f"WHERE datetime BETWEEN '{fetch_date}' AND '{next_fetch_date}'")/(3600*24)
    except ProgrammingError as e:
        print(e)
        return False
    return count >= (next_fetch_date - fetch_date).days


def _extract_symbol_klines(db, timeframes, symbol, start_date, end_date, pbar, data_path, is_local=False):
    symbol = "".join(symbol.split("_"))
    for timeframe in timeframes:
        fetch_date = start_date
        while (end_date - fetch_date).days > 0:
            is_next_month = fetch_date + relativedelta(months=1) <= end_date
            r_delta  = relativedelta(months=1) if is_next_month else relativedelta(days=1)
            path = f"data/spot/monthly/klines/{symbol}/{timeframe}/" if is_next_month else f"data/spot/daily/klines/{symbol}/{timeframe}/"
            next_fetch_date = fetch_date + r_delta
            if not _local_saved(timeframe, fetch_date, symbol, is_next_month, data_path) and not _cloud_saved(timeframe, fetch_date, next_fetch_date, symbol, is_local):
                _get_binance_data(path, db, fetch_date, next_fetch_date, data_path, is_local)
            days_to_update = (next_fetch_date - fetch_date).days if is_next_month else 1
            fetch_date = next_fetch_date
            pbar.update(days_to_update)

def extract_klines(symbols:list, timeframes:list, start_date:datetime, end_date:datetime, is_local:bool=True, db_config_info=None, data_path=None):
    db = utils.Database(db_config_info) if not is_local else None
    data_path = data_path if not data_path is None else "DATA/temp/data"
    with tqdm(total=len(symbols)*(end_date - start_date).days*len(timeframes)) as pbar:
        for symbol in symbols:
            _extract_symbol_klines(db, timeframes, symbol, start_date, end_date, pbar, data_path, is_local)
    res = {}
    for symbol in tqdm(symbols):
        symbol = "".join(symbol.split("_"))
        res[symbol] = {}
        for timeframe in timeframes:
            path = f"{data_path}/{symbol}/{timeframe}"
            if os.path.exists(path):
                try:
                    data_to_save = _get_data_to_save(path)
                    data_to_save.reset_index(drop=True, inplace=True)
                    table_name = f"{symbol}_{timeframe}"
                    if is_local:
                        pass
                        # data_to_save.to_csv(f"{data_path}/{table_name}.csv", index=True)
                    else:
                        db.save_dataframe(data_to_save, table_name, if_exists='append')
                    res[symbol][timeframe] = data_to_save
                    
                    
                except FileNotFoundError as e:
                    print(e)
                    continue
                
    if not is_local:
        utils.delete_temp_data(data_path)
    
    return res