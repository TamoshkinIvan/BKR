import os
import pandas as pd
import math
import numpy as np


csv_names = os.listdir(".")
csv_names.remove('.ipynb_checkpoints')
csv_names.remove('dataset_making.py')
csv_names.remove('data_proc_before_kz.ipynb')
csv_names.remove('data_proc_after_kz.ipynb')
csv_names.remove('new_res')
csv_names.remove('test_osib1')
csv_names.remove('Данные для тестовой схемы')
csv_names.remove('predict.png')



def get_df_tran(csv_names):
    it = 0
    result = pd.DataFrame()
    while it <= 135:
        if it in range(0,len(csv_names),4):
            result = pd.concat([result, pre_processing_apv_data(csv_name=csv_names,i=it)], ignore_index=True)
        elif it in range(1,len(csv_names),4):
            result = pd.concat([result, pre_processing_apv250_data(csv_name=csv_names,i=it)], ignore_index=True)
        elif it in range(2, len(csv_names), 4):
            result = pd.concat([result, pre_processing_kz250_data(csv_name=csv_names,i=it)], ignore_index=True)
        elif it in range(3, len(csv_names), 4):
            result = pd.concat([result, pre_processing_kz300_data(csv_name=csv_names,i=it)], ignore_index=True)
        it += 1
    return result


#transient_111 = pd.read_csv("test_trans.csv", delimiter = ";",  decimal=".")
def pre_processing_apv_data(csv_name, i):
    result = pd.DataFrame()
    w = 0
    delta_1 = 0
    time_1 = 0
    transient = pd.DataFrame()
    time_2 = 0
    transient_56 = pd.DataFrame()

    # Считаем необходимые данные
    transient = pd.read_csv(csv_name[i], delimiter=";",
                                decimal=",", index_col=False)[71:94:]
    transient_56 = pd.read_csv(csv_name[i], delimiter=";",
                                   decimal=",", index_col=False)[68:94:]

    # Избавляемся от выбросов по углу (еще не точно)
    if (transient['delta_horon'] > 400).any():
        return result

        # Выполним расчеты относительно березовской грэс
    transient['delta'] = transient['delta_horon']  # - transient['delta_beraza']
    transient_56['delta'] = transient_56['delta_horon']  # - transient_56['delta_beraza']

        # Переводим данные в в формат numpy
    delta_1 = transient_56["delta"].to_numpy()
    time_1 = transient_56["t"].to_numpy()

        # Избавляемся от плюс минус нуля
    transient = transient.drop(index=[82])

        # Теперь удалим точки появляющиеся при расчете ПП плюс минус ноль
    time_2 = np.delete(time_1, 13)
    delta = np.delete(delta_1, 13)

        # Считаем производную
    w = np.diff(delta) / (np.diff(time_2) * 1000)

        # Удаляем первую строчку, для соблюденяи размерности
    w_1 = np.delete(w, 0)
    w_1 = np.delete(w_1, 0)
    transient["w"] = w_1
    result = pd.concat([result, transient], ignore_index=True)
    i += 4
    return result


def pre_processing_apv250_data(csv_name, i):
    result = pd.DataFrame()
    w = 0
    delta_1 = 0
    time_1 = 0
    transient = pd.DataFrame()
    time_2 = 0
    transient_56 = pd.DataFrame()

    # Считаем необходимые данные
    transient = pd.read_csv(csv_name[i], delimiter=";",
                                decimal=",", index_col=False)[66:89:]
    transient_56 = pd.read_csv(csv_name[i], delimiter=";",
                                   decimal=",", index_col=False)[63:89:]

    # Избавляемся от выбросов по углу (еще не точно)
    if (transient['delta_horon'] > 400).any():
            return result


        # Выполним расчеты относительно березовской грэс
    transient['delta'] = transient['delta_horon']  # - transient['delta_beraza']
    transient_56['delta'] = transient_56['delta_horon']  # - transient_56['delta_beraza']

        # Переводим данные в в формат numpy
    delta_1 = transient_56["delta"].to_numpy()
    time_1 = transient_56["t"].to_numpy()

        # Избавляемся от плюс минус нуля
    transient = transient.drop(index=[77])
        # Теперь удалим точки появляющиеся при расчете ПП плюс минус ноль
    time_2 = np.delete(time_1, 13)
    delta = np.delete(delta_1, 13)

        # Считаем производную
    w = np.diff(delta) / (np.diff(time_2) * 1000)

        # Удаляем первую строчку, для соблюденяи размерности
    w_1 = np.delete(w, 0)
    w_1 = np.delete(w_1, 0)
    transient["w"] = w_1
    result = pd.concat([result, transient], ignore_index=True)
    i += 4
    return result


def pre_processing_kz250_data(csv_name, i):
        result = pd.DataFrame()
        w = 0
        delta_1 = 0
        time_1 = 0
        transient = pd.DataFrame()
        time_2 = 0
        transient_56 = pd.DataFrame()

        # Считаем необходимые данные
        transient = pd.read_csv(csv_name[i], delimiter=";",
                                decimal=",", index_col=False)[29:52:]
        transient_56 = pd.read_csv(csv_name[i], delimiter=";",
                                   decimal=",", index_col=False)[26:52:]
        # Избавляемся от выбросов по углу (еще не точно)
        if (transient['delta_horon'] > 400).any():
            return result

        # Выполним расчеты относительно березовской грэс
        transient['delta'] = transient['delta_horon']  # - transient['delta_beraza']
        transient_56['delta'] = transient_56['delta_horon']  # - transient_56['delta_beraza']

        # Переводим данные в в формат numpy
        delta_1 = transient_56["delta"].to_numpy()
        time_1 = transient_56["t"].to_numpy()

        # Избавляемся от плюс минус нуля
        transient = transient.drop(index=[40])

        # Теперь удалим точки появляющиеся при расчете ПП плюс минус ноль
        time_2 = np.delete(time_1, 13)
        delta = np.delete(delta_1, 13)

        # Считаем производную
        w = np.diff(delta) / (np.diff(time_2) * 1000)

        # Удаляем первую строчку, для соблюденяи размерности
        w_1 = np.delete(w, 0)
        w_1 = np.delete(w_1, 0)
        transient["w"] = w_1
        result = pd.concat([result, transient], ignore_index=True)
        i += 4
        return result

def pre_processing_kz300_data(csv_name, i):
        result = pd.DataFrame()
        w = 0
        delta_1 = 0
        time_1 = 0
        transient = pd.DataFrame()
        time_2 = 0
        transient_56 = pd.DataFrame()
        #Считаем необходимые данные
        transient = pd.read_csv(csv_name[i], delimiter=";",
                                decimal=",", index_col=False)[31:54:]
        transient_56 = pd.read_csv(csv_name[i], delimiter=";",
                                   decimal=",", index_col=False)[28:54:]
        #Избавляемся от выбросов по углу (еще не точно)
        if (transient['delta_horon'] > 400).any():
            return result
        #Выполним расчеты относительно березовской грэс
        transient['delta'] = transient['delta_horon'] #- transient['delta_beraza']
        transient_56['delta'] = transient_56['delta_horon'] #- transient_56['delta_beraza']
        #Переводим данные в в формат numpy
        delta_1 = transient_56["delta"].to_numpy()
        time_1 = transient_56["t"].to_numpy()

        #Избавляемся от плюс минус нуля
        transient = transient.drop(index=[42])
        #Теперь удалим точки появляющиеся при расчете ПП плюс минус ноль
        time_2 = np.delete(time_1, 14)
        delta = np.delete(delta_1, 14)

        #Считаем производную
        w = np.diff(delta) / (np.diff(time_2) * 1000)

        #Удаляем первую строчку, для соблюденяи размерности
        w_1 = np.delete(w, 0)
        w_1 = np.delete(w_1, 0)
        transient["w"] = w_1
        result = pd.concat([result, transient], ignore_index=True)
        return result


df = get_df_tran(csv_names)