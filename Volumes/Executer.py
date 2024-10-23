import os
import time
from datetime import datetime
import seaborn as sns
from Access.AccessInfo import AccessInfo as ai
from sqlalchemy.dialects.mssql.information_schema import columns
from Helpers.ParamsAndFuns import ParamsAndFuns as p
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from Helpers.DbConnector import DbConnector
from Helpers.VolumesHelper import VolumesHelper
import re


p.set_np_pd_opts()
res = ''

#region Параметры подключения
need_load_full = True
need_merge = False
need_save_info = True

host = ai.Host
database = ai.Database
user = ai.User
password = ai.Password
co_df_info = ai.co_df_info
# modelType = 'volumes'
file_dir = ai.file_dir_volumes
plots_dir = ai.plots_dir_volumes
save_name = ai.save_name_volumes
load_name = ai.load_name_volumes
sk_arr = ai.sk_arr

#endregion
#region Получение датафрейма

#Если есть датафрейм
if need_load_full:
    volHel = VolumesHelper(file_dir + load_name)
    dfFull = volHel.fullDf

#Если нужно объединить несколько датафреймов в один
elif(need_merge):
    dfFull = p.load_few_df(ai.file_dir_volumes,ai.concat_name_volumes)
    dfFull.to_csv(file_dir + save_name, sep=';', index_label=False)
    volHel = VolumesHelper(file_dir + save_name)
    p.save_log(dfFull, ai.sk_short_info, ai.file_dir_volumes)

#Если получаем впервые выгрузку из БД
else:
    # Создаем экземпляр для выгрузки из БД
    dbCon = DbConnector(host, user, password, database)
    # Подгружаем датафрейм по помещениям и сохраням
    dfFull = dbCon.get_volumes_df(co_df_info= co_df_info, sk_arr= sk_arr)
    dfFull.to_csv(file_dir + save_name, sep=';', index_label=False)
    volHel = VolumesHelper(file_dir + save_name)
    dfFull = volHel.fullDf
    p.save_log(volHel.fullDf, ai.sk_short_info, ai.file_dir_volumes)
#endregion

#region Получение эталонов
# Создаем экземпляр для выгрузки из БД
standarts_dict = volHel.get_standarts(dfFull=dfFull,sk_arr= sk_arr,param_name='Объем, м3')
deviation_dict = volHel.get_df_arr_sk_dev(dfFull= dfFull,sk_arr= sk_arr, param_name='Объем, м3', co_df_info=co_df_info)

print(standarts_dict[sk_arr[0]])

#Сохранение
if(need_save_info):
    volHel.save_standarts(standarts_dict, sk_arr, ai.file_dir_volumes, f'Эталоны')
    volHel.save_standarts(deviation_dict, sk_arr, ai.file_dir_volumes, f'Отклонения')
    volHel.save_boxplotes_for_morph_and_floor_types(sk_arr=sk_arr
                                                        ,df_full=dfFull
                                                        ,pref_name=ai.sk_short_info
                                                        ,dir=ai.plots_dir_volumes)
    export_df = dfFull[['Имя СК', 'Материал', 'Наименование', 'Объем, м3', 'Оси'
        , 'Толщина', 'Формат', 'Секция', 'Этаж', 'version_index', 'name', 'Морфотип секции', 'Тип этажа']]
    export_df.to_excel(ai.file_dir_volumes + fr'\{ai.sk_short_info}.xlsx', sheet_name=ai.short_name_prem, index=False)


#endregion

# #66.946408075
# outer = dfFull[dfFull['Имя СК']=='Внешняя стена. Кладка']
# res = outer[(outer['Морфотип секции'] == 'B_CS(CN)5.8')
#             & (outer['construction_object_id'] == '6016cd2b-68b4-4f26-88eb-171ffbb662d3')
#             & (outer['Этаж'] == 'Этаж 04')
#             & (outer['Тип этажа'] == 'Типовой этаж')][['name','version_index','Секция','Этаж','Объем, м3']] ['Объем, м3'].sum()
#
# res = outer.groupby(['Морфотип секции','Секция','construction_object_id', 'Этаж', 'Тип этажа']).sum(numeric_only=True)['Объем, м3'].reset_index()
# res = res[(res['Морфотип секции'] == 'B_CS(CN)5.8')
#             & (res['construction_object_id'] == '6016cd2b-68b4-4f26-88eb-171ffbb662d3')
#             & (res['Этаж'] == 'Этаж 04')
#             & (res['Тип этажа'] == 'Типовой этаж')]




# plt.show()
print(res)

