import os
import pandas as pd
import math
import numpy as np

csv_names = os.listdir("new_res")


def get_df_tran(csv_names, window_size):
    result = pd.DataFrame()
    time_fall_list = []
    is_fall_list = []
    contingency_1 = []
    full_fall_list = []
    full_time = []
    for i in range(0, len(csv_names)):
        dict_result_1 = pre_processing(csv_name=csv_names[i], window_size=window_size)
        result = pd.concat([result,
                            dict_result_1['res']],
                            ignore_index=True)
        time_fall_list.append(dict_result_1['time'])
        is_fall_list.append(dict_result_1['is_fall'])
        contingency_1.append(dict_result_1['cont'])
        full_time.append(dict_result_1['time_full'])
        full_fall_list.append(dict_result_1['full_fall'])
        i += 1
    total_result = {
                    'data': result,
                    'time_lst': time_fall_list,
                    'is_fall': is_fall_list,
                    'cont1': contingency_1,
                    'full_fall_list': full_fall_list,
                    'full_time': full_time
                    }

    time = pd.DataFrame(total_result['time_lst'])
    time_full = pd.DataFrame(total_result['full_time'])
    print(total_result['data'].info())
    print(f"Количество случаев превышения угла 180 градусов: {sum(total_result['is_fall'])}")
    print(f"Общее количество рассмотренных возмущений {sum(total_result['cont1'])}")
    print(f"Отношение процент случаев нарушения устойчивости { sum(total_result['is_fall']) / sum(total_result['cont1']) * 100 }")
    print(time_full.describe())
    print(time.describe())
    return total_result



def pre_processing(csv_name, window_size):
    #Данные, которые нам нужны для исследования
    #Размер входного окна
    windows_size_ref = int(window_size / (10))
    #Величина на которую отступаем влево, относительно окончания возмущения
    left_border = windows_size_ref + 3 + 10
    # Величина на которую отступаем влево, относительно окончания возмущения
    right_border = windows_size_ref + 2 + 10
    #Ведем подсчет сколько по итогу возмущений использовалось для обучения и тестирования
    contingency = 0
    #Итоговый датафрейм
    result = pd.DataFrame()
    #Определяем было ли нарушение устойчивости
    is_stability_fall = 0
    #Определяем время нарушения устойчивости
    stabilit_fall_time = None
    #Определяем время нарушения устойчивости полный проворот
    full_stabilit_fall_time = None
    #Полный проворот
    full_fall = 0
    #Возвращаемый словарь
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont': contingency,
                    'time_full': full_stabilit_fall_time,
                    'full_fall': full_fall
                    }
    #Список с точками плюс минус ноль
    trans_list = []
    # Преобразуем полученные данные в датафрейм
    transient_init = pd.read_csv(f'new_res/{csv_name}',
                            delimiter=";",
                            decimal=",",
                            index_col=False,
                            on_bad_lines='warn')
    #Ищем индекс, который от которого будем вести отсчет, обозначет окончание возмущения
    for index, row in transient_init.iterrows():
        if index == 0:
            continue
        if transient_init['t'].loc[index] - transient_init['t'].loc[index - 1] < 0.001:
            index_start = index
            trans_list.append(index_start)

    # Считаем необходимые данные
    transient = transient_init[index_start - left_border : index_start + right_border:]
    transient_56 = transient_init[index_start - left_border - 3 :index_start + right_border:]

    # Избавляемся от плюс минус нуля
    transient = transient.drop(index_start)
    transient_56 = transient_56.drop(index_start)

    # Задаем новые индексы
    transient = transient.reset_index(drop=True)
    transient_56 = transient_56.reset_index(drop=True)

    # Выполним расчеты относительно березовской грэс
    transient.loc[:,'delta'] =  transient['delta_horon'] #- transient['delta_vartovsk']
    transient_56.loc[:, 'delta'] = transient_56['delta_horon'] #- transient_56['delta_vartovsk']

    # Переводим данные в в формат numpy
    time_1 = transient_56['t'].to_numpy()
    delta_1 = transient_56['delta'].to_numpy()

    # Считаем производную
    w = np.diff(delta_1) / (np.diff(time_1) * 1000)

    # Удаляем первую строчку, для соблюденяи размерности
    w_1 = np.delete(w, 0)
    w_1 = np.delete(w_1, 0)
    transient["w"] = w_1

    # Определяем время нарушения устойчивости и факт нарушения устойчивости
    for index, row in transient.iterrows():
        if row['delta'] > 180:
            is_stability_fall = 1
            stabilit_fall_time = (row['t'] - transient_init['t'].loc[index_start]) * 1000
            break

    # Определяем время проворота на 360
    for index, row in transient.iterrows():
        if row['delta'] > 250:
            full_fall = 1
            full_stabilit_fall_time = (row['t'] - transient_init['t'].loc[index_start]) * 1000
            break

    # Возращаем единичку, т.к. у нас это возмущение идет в обущающую выборку
    contingency += 1

    #Получаем итоговый датафрейм
    result = pd.concat([result, transient], ignore_index=True)
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stabilit_fall_time,
                    'cont': contingency,
                    'time_full': full_stabilit_fall_time,
                    'full_fall': full_fall
                    }
    return dict_result

df = get_df_tran(csv_names, window_size=200)