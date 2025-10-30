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
from typing import List




SAVING_DIR = "./DATA/temp/data"


def _local_saved(fetch_date, symbol, is_next_month, data_path):
    month = "0" + str(fetch_date.month) if fetch_date.month < 10 else str(fetch_date.month)
    day = "0" + str(fetch_date.day) if fetch_date.day < 10 else str(fetch_date.day)
    fetch_date_str = f"{fetch_date.year}-{month}"
    if not is_next_month: fetch_date_str += f"-{day}"
    return os.path.exists(f"{data_path}/fundingRate/{symbol}/{symbol}-fundingRate-{fetch_date_str}.csv")

def _cloud_saved(fetch_date, next_fetch_date, symbol, is_local):
    if is_local == True:
        return False

def _extract_product_fundings(db, product, start_date, end_date, pbar, data_path, is_local, rotation=False):
    market = utils.MARKET_MAPPING.get(product.split("_")[1], None)
    if market is None:
        raise exceptions.FailedToExtractException(f"Failed to categorized product {product}")
    symbol = "".join(product.split("_"))
    fetch_date = start_date
    while (end_date - fetch_date).days > 0:
        is_next_month = fetch_date + relativedelta(months=1) <= end_date
        r_delta  = relativedelta(months=1) if is_next_month else relativedelta(days=1)
        path = f"data/futures/{market}/monthly/fundingRate/{symbol}/" if is_next_month else f"data/futures/{market}/daiky/fundingRate/{symbol}/"
        next_fetch_date = fetch_date + r_delta
        if not _local_saved(fetch_date, symbol, is_next_month, data_path) and not _cloud_saved(fetch_date, next_fetch_date, symbol, is_local):
            utils.get_binance_data(path, db, fetch_date, next_fetch_date, data_path, is_local, rotation=rotation)
        days_to_update = (next_fetch_date - fetch_date).days if is_next_month else 1
        fetch_date = next_fetch_date
        pbar.update(days_to_update)

def _extract_binance_historical_data(raw_binance_df):
    df =  pd.DataFrame({
        "timestamp":raw_binance_df[list(raw_binance_df.columns)[0]],
        "funding_rate":raw_binance_df[list(raw_binance_df.columns)[2]],
    })
    df.reset_index(drop=True, inplace=True)
    try:
        df["timestamp"] = [int(str(d)[:-3] + "000") for d in df['timestamp'].tolist()]
    except ValueError:
        df = df.loc[1:]
        df["timestamp"] = [int(str(d)[:-3] + "000") for d in df['timestamp'].tolist()]
    df['datetime'] =  pd.to_datetime(df["timestamp"],unit='ms')
    return df


def extract_fundings(products:List[str], start_date:datetime, end_date:datetime, is_local:bool=True, db_config_info=None, data_path=None, saving_data_path=None, rotation=False):
    db = utils.Database(db_config_info) if not is_local else None
    data_path = data_path if not data_path is None else "DATA/temp/data"
    saving_data_path = saving_data_path if not saving_data_path is None else "DATA"
    
    with tqdm(total=len(products)*(end_date - start_date).days) as pbar:
        for product in products:
            if len(product.split("_")) != 2:
                raise exceptions.FailedToExtractException(f"Unsplitable product : {product}")
            _extract_product_fundings(db, product, start_date, end_date, pbar, data_path, is_local,rotation=rotation)
    res = {}
    for product in tqdm(products):
        product_standarized = product
        product = "".join(product.split("_"))
        res[product_standarized] = {}
        path = f"{data_path}/fundingRate/{product}"
        if os.path.exists(path):
            try:
                data_to_save = utils.get_data_to_save(path, _extract_binance_historical_data)
                print(data_to_save.shape)
                data_to_save.reset_index(drop=True, inplace=True)
                table_name = f"{product}"
                if is_local:
                    data_to_save.to_csv(f"{saving_data_path}/{table_name}.csv", index=False)
                else:
                    db.save_dataframe(data_to_save, table_name, if_exists='append')
                res[product_standarized] = data_to_save
                
                
            except FileNotFoundError as e:
                print(e)
                continue
                
    if not is_local:
        utils.delete_temp_data(data_path)
    
    return res