import os
import pandas as pd
import math
import numpy as np

csv_names = os.listdir("data")


def get_df_tran(csv_names):

    i = 0
    result = pd.DataFrame()
    time_fall_list = []
    is_fall_list = []
    contingency_1 = []
    contingency_2 = []
    contingency_3 = []
    contingency_4 = []
    while i <= 135:
        if i in range(0,len(csv_names),4):
            dict_result_1 = pre_processing_apv_data(csv_name=csv_names[i])
            result = pd.concat([result,
                                dict_result_1['res']],
                               ignore_index=True)
            time_fall_list.append(dict_result_1['time'])
            is_fall_list.append(dict_result_1['is_fall'])
            contingency_1.append(dict_result_1['cont_1'])
        elif i in range(1,len(csv_names),4):
            dict_result_2 = pre_processing_apv250_data(csv_name=csv_names[i])
            result = pd.concat([result,
                                dict_result_2['res']],
                               ignore_index=True)
            time_fall_list.append(dict_result_2['time'])
            is_fall_list.append(dict_result_2['is_fall'])
            contingency_2.append(dict_result_2['cont_2'])
        elif i in range(2, len(csv_names), 4):
            dict_result_3 = pre_processing_kz250_data(csv_name=csv_names[i])
            result = pd.concat([result,
                                dict_result_3['res']],
                               ignore_index=True)
            time_fall_list.append(dict_result_3['time'])
            is_fall_list.append(dict_result_3['is_fall'])
            contingency_3.append(dict_result_3['cont_3'])
        elif i in range(3, len(csv_names), 4):
            dict_result_4 = pre_processing_kz300_data(csv_name=csv_names[i])
            result = pd.concat([result,
                                dict_result_4['res']],
                               ignore_index=True)
            time_fall_list.append(dict_result_4['time'])
            is_fall_list.append(dict_result_4['is_fall'])
            contingency_4.append(dict_result_4['cont_4'])
        i += 1
    total_result = {
                    'data': result,
                    'time_lst': time_fall_list,
                    'is_fall': is_fall_list,
                    'cont1': contingency_1,
                    'cont2': contingency_2,
                    'cont3': contingency_3,
                    'cont4': contingency_4,
                    }
    print(total_result['data'].info())
    print(f"Количество случаев превышения угла 180 градусов: {sum(total_result['is_fall'])}")
    print(sum(total_result['cont1']))
    print(sum(total_result['cont2']))
    print(sum(total_result['cont3']))
    print(sum(total_result['cont4']))
    print(total_result['time_lst'])
    return total_result



def pre_processing_apv_data(csv_name):

    #Данные, которые нам нужны для исследования
    contingency_1 = 0
    result = pd.DataFrame()
    is_stability_fall = 0
    stabilit_fall_time = None
    #Возвращаемый словарь
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont_1': contingency_1
                    }

    w = 0
    delta_1 = 0
    time_1 = 0
    transient = pd.DataFrame()
    time_2 = 0
    transient_56 = pd.DataFrame()

    # Считаем необходимые данные
    transient = pd.read_csv(f'data/{csv_name}',
                            delimiter=";",
                            decimal=",",
                            index_col=False)[71:94:]
    transient_56 = pd.read_csv(f'data/{csv_name}',
                               delimiter=";",
                                decimal=",",
                               index_col=False)[68:94:]

    # Избавляемся от выбросов по углу (еще не точно)
    if (transient['delta_horon'][:8] > 180).any():
    #if (transient['delta_horon'] > 400).any():
        return dict_result

    # Выполним расчеты относительно березовской грэс
    transient['delta'] = transient['delta_horon']
    transient_56['delta'] = transient_56['delta_horon']

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

    # Определяем время нарушения устойчивости и факт нарушения устойчивости
    for index, row in transient.iterrows():
        if row['delta'] > 180:
            is_stability_fall = 1
            stabilit_fall_time = (row['t'] - 1.5) * 1000
            break

    contingency_1 += 1
    #Получаем итоговый датафрейм
    result = pd.concat([result, transient], ignore_index=True)
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont_1': contingency_1
                    }
    return dict_result



def pre_processing_apv250_data(csv_name):
    #Данные, которые нам нужны для исследования
    result = pd.DataFrame()
    is_stability_fall = 0
    stabilit_fall_time = None
    contingency_2 = 0
    #Возвращаемый словарь
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont_2': contingency_2
                    }
    w = 0
    delta_1 = 0
    time_1 = 0
    transient = pd.DataFrame()
    time_2 = 0
    transient_56 = pd.DataFrame()
    # Считаем необходимые данные
    transient = pd.read_csv(f'data/{csv_name}',
                            delimiter=";",
                                decimal=",",
                            index_col=False)[66:89:]
    transient_56 = pd.read_csv(f'data/{csv_name}',
                               delimiter=";",
                               decimal=",",
                               index_col=False)[63:89:]

    # Избавляемся от выбросов по углу (еще не точно)
    if (transient['delta_horon'][:8] > 180).any():
    #if (transient['delta_horon'] > 400).any():
            return dict_result
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
    # Определяем время нарушения устойчивости и факт нарушения устойчивости
    for index, row in transient.iterrows():
        if row['delta'] > 180:
            is_stability_fall = 1
            stabilit_fall_time = (row['t'] - 1.4) * 1000
            break
    #Получаем итоговый датафрейм
    contingency_2 += 1
    result = pd.concat([result, transient], ignore_index=True)
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont_2': contingency_2
                    }
    return dict_result



def pre_processing_kz250_data(csv_name):
        # Данные, которые нам нужны для исследования
        result = pd.DataFrame()
        is_stability_fall = 0
        stabilit_fall_time = None
        contingency_3 = 0
        # Возвращаемый словарь
        dict_result = {
                        'res': result,
                        'is_fall': is_stability_fall,
                        'time': stabilit_fall_time,
                        'cont_3': contingency_3
                        }

        w = 0
        delta_1 = 0
        time_1 = 0
        transient = pd.DataFrame()
        time_2 = 0
        transient_56 = pd.DataFrame()
        # Считаем необходимые данные
        transient = pd.read_csv(f'data/{csv_name}',
                                delimiter=";",
                                decimal=",",
                                index_col=False)[29:52:]
        transient_56 = pd.read_csv(f'data/{csv_name}',
                                   delimiter=";",
                                   decimal=",",
                                   index_col=False)[26:52:]
        # Избавляемся от выбросов по углу (еще не точно)
        if (transient['delta_horon'][:8] > 180).any():
        #if (transient['delta_horon'] > 400).any():
            return dict_result
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
        # Определяем время нарушения устойчивости и факт нарушения устойчивости
        for index, row in transient.iterrows():
            if row['delta'] > 180:
                is_stability_fall = 1
                stabilit_fall_time = (row['t'] - 0.75) * 1000
                break
        # Получаем итоговый датафрейм
        result = pd.concat([result, transient], ignore_index=True)
        contingency_3 += 1
        dict_result = {
                        'res': result,
                        'is_fall': is_stability_fall,
                        'time': stabilit_fall_time,
                        'cont_3': contingency_3
                        }
        return dict_result



def pre_processing_kz300_data(csv_name):
        # Данные, которые нам нужны для исследования
        result = pd.DataFrame()
        is_stability_fall = 0
        stabilit_fall_time = None
        contingency_4 = 0
        # Возвращаемый словарь
        dict_result = {
                        'res': result,
                        'is_fall': is_stability_fall,
                        'time': stabilit_fall_time,
                        'cont_4': contingency_4
                     }
        w = 0
        delta_1 = 0
        time_1 = 0
        transient = pd.DataFrame()
        time_2 = 0
        transient_56 = pd.DataFrame()
        #Считаем необходимые данные
        transient = pd.read_csv(f'data/{csv_name}',
                                delimiter=";",
                                decimal=",",
                                index_col=False)[31:54:]
        transient_56 = pd.read_csv(f'data/{csv_name}',
                                   delimiter=";",
                                   decimal=",",
                                   index_col=False)[28:54:]
        #Избавляемся от выбросов по углу (еще не точно)
        if (transient['delta_horon'][:8] > 180).any():
        #if (transient['delta_horon'] > 400).any():
            return dict_result
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
        # Определяем время нарушения устойчивости и факт нарушения устойчивости
        for index, row in transient.iterrows():
            if row['delta'] > 180:
                is_stability_fall = 1
                stabilit_fall_time = (row['t'] - 0.8) * 1000
                break
        # Получаем итоговый датафрейм
        result = pd.concat([result, transient], ignore_index=True)
        contingency_4 += 1
        dict_result = {
                        'res': result,
                        'is_fall': is_stability_fall,
                        'time': stabilit_fall_time,
                        'cont_4': contingency_4
                     }
        return dict_result


df = get_df_tran(csv_names)