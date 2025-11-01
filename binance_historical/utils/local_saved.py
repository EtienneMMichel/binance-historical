from enum import Enum
import os
from ..exceptions import LocalSavedException



class LocalSavedModes(Enum):
    TIMEFRAME = 1
    FUNDING = 2

def is_local_saved(data_path, symbol, fetch_date, mode, mode_value=None, is_month_fully_completed=True):
    month = str(fetch_date.month).zfill(2)
    fetch_date_str = f"{fetch_date.year}-{month}"
    if is_month_fully_completed:
        day = str(fetch_date.day).zfill(2)
        fetch_date_str += f"-{day}"
    if mode == LocalSavedModes.TIMEFRAME:
        if mode_value is None:
            raise LocalSavedException("please provide a timeframe value inside 'mode_value' argument")
        fetching_data_path = f"{data_path}/{symbol}/{mode_value}/{symbol}-{mode_value}-{fetch_date_str}.csv"
    elif mode == LocalSavedModes.FUNDING:
        fetching_data_path = f"{data_path}/{symbol}/{symbol}-fundingRate-{fetch_date_str}.csv"
    else:
        raise LocalSavedException(f"unknow mode {mode}")
    print("fetching_data_path: ", fetching_data_path)
    print("exist: ", os.path.exists(fetching_data_path))
    return os.path.exists(fetching_data_path)