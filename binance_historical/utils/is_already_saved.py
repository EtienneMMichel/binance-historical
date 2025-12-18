from enum import Enum
import os
from ..exceptions import SavedException



class SavedModes(Enum):
    TIMEFRAME = 1
    FUNDING = 2

def is_already_saved(data_path, symbol, fetch_date, mode, mode_value=None, is_month_fully_completed=True, is_local=True):
    month = str(fetch_date.month).zfill(2)
    fetch_date_str = f"{fetch_date.year}-{month}"
    if is_month_fully_completed:
        day = str(fetch_date.day).zfill(2)
        fetch_date_str += f"-{day}"
    if mode == SavedModes.TIMEFRAME:
        if mode_value is None:
            raise SavedException("please provide a timeframe value inside 'mode_value' argument")
        fetching_data_path = f"{data_path}/{symbol}/{mode_value}/{symbol}-{mode_value}-{fetch_date_str}.csv"
    elif mode == SavedModes.FUNDING:
        fetching_data_path = f"{data_path}/{symbol}/{symbol}-fundingRate-{fetch_date_str}.csv"
    else:
        raise SavedException(f"unknow mode {mode}")
    print("fetching_data_path: ", fetching_data_path)
    print("exist: ", os.path.exists(fetching_data_path))
    if is_local:
        return os.path.exists(fetching_data_path)
    else:
        return is_cloud_saved(fetching_data_path)


def is_cloud_saved(data_path):
    raise NotImplementedError()