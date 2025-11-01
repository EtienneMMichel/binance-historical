from binance_historical.extract import extract_klines
import datetime as dt

def test_extract_fundings():
    is_local = True
    symbols = ["BTC_USDT", "ETH_USDT"]
    timeframes = ["8h"]
    start_date = dt.datetime.strptime("2024-01-01", format="%Y-%m-%d")
    end_date = dt.datetime.now()
    market="futures"
    rotation = False
    extract_klines(symbols=symbols, timeframes=timeframes, start_date=start_date, end_date=end_date, is_local=is_local, market=market, rotation=rotation)


if __name__ == "__main__":
    test_extract_fundings()