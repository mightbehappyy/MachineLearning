import pandas as pd
import yfinance as yf

class Extractor():

    def __init__(self):
        self.raw_path = '/home/pedro/AulaMachineLearning/machinelearning/data/raw/'


    def download_data(self, symbols: list, start_date: str, end_date: str):
        for symbol in symbols:
            temp_path = self.raw_path+symbol+'.parquet.gzip'
            try:
                df = yf.download(symbol, start=start_date, end=end_date)
                if df.shape[0] == 0:
                    print(f"A base {symbol} não está disponível")
                else:
                    df['Name'] = symbol
                    df.to_parquet(temp_path, compression='gzip')
            except Exception as e:
                print("Ocorreu um erro ao fazer o download em download_data: ", e)

extractor = Extractor() 
extractor.download_data(['BTC-USD', 'ETH-USD', 'USDT-USD', 'SOL-USD', 'DOGE-USD'], '2018-01-01', '2023-11-29')