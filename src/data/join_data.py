
import pandas as pd
import glob

def data_append():
    path = "/home/pedro/AulaMachineLearning/machinelearning/data/raw"
    path_pattern = path + "/*.parquet.gzip"

    lista_paths = glob.glob(path_pattern)
    lista_dfs = []
    for file in lista_paths:
        df = pd.read_parquet(file)
        lista_dfs.append(df)



    df_appended = pd.concat(lista_dfs, ignore_index=True)
    path_to_save = "/home/pedro/AulaMachineLearning/machinelearning/data/processed/"
    df_appended.to_parquet(path_to_save+"fato_appended.parquet.gzip", compression="gzip")

data_append()
