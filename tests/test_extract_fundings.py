from binance_historical.extract import extract_fundings
import datetime as dt

def test_extract_fundings():
    is_local = True
    products = ["BTC_USDC", "ETH_USDC"]
    start_date = dt.datetime.now() - dt.timedelta(days=365)
    end_date = dt.datetime.now()
    extract_fundings(products=products,
                     start_date=start_date,
                     end_date=end_date,
                     is_local=is_local)
    
# def test_check_


if __name__ == "__main__":
    test_extract_fundings()