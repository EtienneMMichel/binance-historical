import requests
import xmltodict
import os
from tqdm import tqdm
from .. import utils

import pandas as pd
from datetime import datetime
import math
from dateutil.relativedelta import relativedelta
from sqlalchemy.exc import ProgrammingError
from .. import exceptions

SAVING_DIR = "./DATA/temp/data"



def _extract_binance_historical_data(raw_binance_df):
    df =  pd.DataFrame({
        "timestamp":raw_binance_df[list(raw_binance_df.columns)[0]],
        "open_price":raw_binance_df[list(raw_binance_df.columns)[1]],
        "high_price":raw_binance_df[list(raw_binance_df.columns)[2]],
        "low_price":raw_binance_df[list(raw_binance_df.columns)[3]],
        "close_price":raw_binance_df[list(raw_binance_df.columns)[4]],
        "volume":raw_binance_df[list(raw_binance_df.columns)[5]],
        "count":raw_binance_df[list(raw_binance_df.columns)[8]],
        "buy_volume":raw_binance_df[list(raw_binance_df.columns)[9]],
        "sell_volume":raw_binance_df[list(raw_binance_df.columns)[10]]
    })
    df.reset_index(inplace=True, drop=True)
    try:
        df['timestamp'] =  df['timestamp'].astype(int)*eval(f"1e-{len(str(df['timestamp'].tolist()[0])) - 10}")
    except ValueError:
        df = df.loc[1:]
        df['timestamp'] =  df['timestamp'].astype(int)*eval(f"1e-{len(str(df['timestamp'].tolist()[0])) - 10}")
    df["timestamp"] = [math.ceil(d) for d in df['timestamp'].tolist()]
    df['datetime'] =  pd.to_datetime(df["timestamp"],unit='s')
    return df



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


def _extract_symbol_klines(db, timeframes, symbol, start_date, end_date, pbar, data_path, is_local=False,market="spot", rotation=False):
    market_type = None
    print("market: ", market)
    if market == "futures":
        market_type = utils.MARKET_MAPPING.get(symbol.split("_")[1], None)
        if market_type is None:
            raise exceptions.FailedToExtractException(f"Failed to categorized symbol {symbol}")
    symbol = "".join(symbol.split("_"))
    for timeframe in timeframes:
        fetch_date = start_date
        while (end_date - fetch_date).days > 0:
            is_month_fully_completed = fetch_date + relativedelta(months=1) > end_date
            if market == "spot":
                path = f"data/spot/monthly/klines/{symbol}/{timeframe}/" if not is_month_fully_completed else f"data/spot/daily/klines/{symbol}/{timeframe}/"
            else:
                path = f"data/futures/{market_type}/monthly/klines/{symbol}/{timeframe}/" if not is_month_fully_completed else f"data/futures/{market_type}/daily/klines/{symbol}/{timeframe}/"

            r_delta  = relativedelta(months=1) if not is_month_fully_completed else relativedelta(days=1)
            next_fetch_date = fetch_date + r_delta
            if is_local==False and not utils.is_already_saved(data_path, symbol, fetch_date, mode=utils.SavedModes.TIMEFRAME, mode_value=timeframe, is_month_fully_completed=is_month_fully_completed, is_local=True):
                utils.get_binance_data(path, db, fetch_date, next_fetch_date, data_path, is_local, rotation=rotation)
            elif not utils.is_already_saved(data_path, symbol, fetch_date, mode=utils.SavedModes.TIMEFRAME, mode_value=timeframe, is_month_fully_completed=is_month_fully_completed, is_local=True):
                utils.get_binance_data(path, db, fetch_date, next_fetch_date, data_path, is_local, rotation=rotation)
            days_to_update = (next_fetch_date - fetch_date).days if not is_month_fully_completed else 1
            fetch_date = next_fetch_date
            pbar.update(days_to_update)

def extract_klines(symbols:list, timeframes:list, start_date:datetime, end_date:datetime, is_local:bool=True, db_config_info=None, data_path=None, saving_data_path=None, market="spot", rotation=False):
    db = utils.Database(db_config_info) if not is_local else None
    data_path = f"DATA/temp/data/klines/{market}"
    saving_data_path = saving_data_path if not saving_data_path is None else "DATA"
    with tqdm(total=len(symbols)*(end_date - start_date).days*len(timeframes)) as pbar:
        for symbol in symbols:
            _extract_symbol_klines(db, timeframes, symbol, start_date, end_date, pbar, data_path, is_local,market, rotation=rotation)
    res = {}
    for symbol in tqdm(symbols):
        symbol_standarized = symbol
        symbol = "".join(symbol.split("_"))
        res[symbol_standarized] = {}
        for timeframe in timeframes:
            path = f"{data_path}/{symbol}/{timeframe}"
            if os.path.exists(path):
                try:
                    data_to_save = utils.get_data_to_save(path, _extract_binance_historical_data)
                    data_to_save.reset_index(drop=True, inplace=True)
                    table_name = f"{market}--{symbol}--{timeframe}"
                    if is_local:
                        data_to_save.to_csv(f"{saving_data_path}/{table_name}.csv", index=True)
                    else:
                        db.save_dataframe(data_to_save, table_name, if_exists='append')
                    res[symbol_standarized][timeframe] = data_to_save
                    
                    
                except FileNotFoundError as e:
                    print(e)
                    continue
                
    if not is_local:
        utils.delete_temp_data(data_path)
    
    return res