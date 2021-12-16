from datetime import date, datetime, timedelta
from re import S
import MetaTrader5 as mt5

import pandas as pd

import fire

def get_symbols(filter='BOVESPA\\A VISTA'):
    symbols = mt5.symbols_get()
    if filter is not None:
        symbols = [info.name for info in symbols if ((filter in info.path) and (info.volume >= 1))]
    print(f'Founded {len(symbols)} symbols.')

    if 'IBOV' not in symbols:
        symbols.append('IBOV')

    return symbols

class get_data():
    def __init__(self, login=52906749, server='XPMT5-DEMO', password='HMgJnurcC5RcRSE2'):
        # exibimos dados sobre o pacote MetaTrader5
        print("MetaTrader5 package author: ",mt5.__author__)
        print("MetaTrader5 package version: ",mt5.__version__)
        # estabelecemos a conexão ao MetaTrader 5
        if not mt5.initialize(login=login, server=server, password=password):
            print("initialize() failed, error code =",mt5.last_error())
            quit()

        # Data to fetch
        self.data = pd.DataFrame()

    def shutdown(self):
        # concluímos a conexão ao terminal MetaTrader 5
        mt5.shutdown()

    def save_data(self, filename='PETR4'):
        file_path=f'data/raw/{filename}.csv'
        self.data.drop_duplicates().to_csv(file_path, index=False)
        print(f'\nDados salvos em {file_path}')

    def fetch_data(self, start_date=datetime(year=2010, month=1, day=1), end_date=datetime.now(), symbol='PETR4', interval=mt5.TIMEFRAME_M1, loop_step=30):
        step = timedelta(days=loop_step)
        print(f'\nFetching {symbol} in range {start_date} - {end_date}\n')
        while (start_date < end_date):
            print(f'{start_date} - {start_date + step}')  
            rates = self.fetch_data_from_remote(start_date, start_date + step, symbol, interval)
            self.data = pd.concat([self.data, rates], ignore_index=True)
            start_date = start_date + step
            
    def fetch_data_from_remote(self, start_date, end_date, symbol, interval):
        if start_date > end_date:
            return None  
        
        # obtemos as barras com USDJPY M5 no intervalo 2020.01.10 00:00 - 2020.01.11 13:00 no fuso horário UTC
        rates = mt5.copy_rates_range(symbol, interval, start_date, end_date)

        # criamos a partir dos dados obtidos DataFrame
        rates_frame = pd.DataFrame(rates)
        # convertemos o tempo em segundos no formato datetime
        rates_frame['time']=pd.to_datetime(rates_frame['time'], unit='s')
        
        return rates_frame

def main():
    fetcher = get_data()

    for symbol in get_symbols():
        fetcher.fetch_data(symbol=symbol, interval=mt5.TIMEFRAME_M10)
        fetcher.save_data(filename=symbol)
    fetcher.shutdown

if __name__=='__main__':
    fire.Fire(main)
