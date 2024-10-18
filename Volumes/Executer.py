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
host = ai.Host
database = ai.Database
user = ai.User
password = ai.Password

already_saved = True
coId= ai.co_id_volumes
stage = ai.stage_volumes
modelType = 'volumes'
file_dir = ai.file_dir_volumes
plots_dir = ai.plots_dir_volumes
save_name = ai.save_name_volumes
load_name = ai.load_name_volumes
sk_arr = ai.sk_arr

#endregion
#region Получение датафрейма
#Получаем датафрейм и сохраняем в csv
if already_saved:
    volHel = VolumesHelper(file_dir + load_name)
    dfFull = volHel.fullDf
else:
    # Создаем экземпляр для выгрузки из БД
    dbCon = DbConnector(host, user, password, database)
    # Подгружаем датафрейм по помещениям и сохраням
    dfFull = dbCon.get_volumes_df(coId, stage, sk_arr)
    dfFull.to_csv(file_dir + save_name, sep=';', index_label=False)
    volHel = VolumesHelper(file_dir + save_name)
#Получаем датафрейм
dfFull = volHel.fullDf
#endregion
#region Проверка данных

res = dfFull

# name                   version_index
# UKV_GP11_ALL_AR_SHD    52.0             5107
# AKD_GP4_ALL_AR_PL_WIP  49.0             5067
# MED_GP02_ALL_AR_PL_RD  62.0             3859
# DVT03_ALL_AR_CP_WIP    48.0             3735
# AKD_GP4_ALL_AR_FR_WIP  38.0             2116
# MED_GP02_ALL_AR_FR_RD  80.0             1974
# DVT03_ALL_AR_CA_WIP    42.0             1913
res = dfFull[['name','version_index']].value_counts()


#Проверка секций
#                      name    Секция           Морфотип секции  count
# 56  AKD_GP4_ALL_AR_FR_WIP   Паркинг  Parking2_(2101m2-2500m2)      2
# 21  AKD_GP4_ALL_AR_FR_WIP  Секция 1              С_CS(CN)7.10    389
# 34  AKD_GP4_ALL_AR_FR_WIP  Секция 2               С_CS(CN)7.8    297
# 41  AKD_GP4_ALL_AR_FR_WIP  Секция 3               C_EW(NS)8.4    197
# 46  AKD_GP4_ALL_AR_FR_WIP  Секция 4               C_EW(NS)8.4    187
# 26  AKD_GP4_ALL_AR_FR_WIP  Секция 5               C_CS(CN)5.8    358
# 28  AKD_GP4_ALL_AR_FR_WIP  Секция 6              С_CS(CN)7.10    350
# 52  AKD_GP4_ALL_AR_FR_WIP  Секция 7               C_EW(NS)6.4    134
# 39  AKD_GP4_ALL_AR_FR_WIP  Секция 8               C_EW(NS)8.4    202
# 54  AKD_GP4_ALL_AR_PL_WIP   Паркинг  Parking2_(2101m2-2500m2)     30
# 1   AKD_GP4_ALL_AR_PL_WIP  Секция 1              С_CS(CN)7.10    931
# 6   AKD_GP4_ALL_AR_PL_WIP  Секция 2               С_CS(CN)7.8    746
# 14  AKD_GP4_ALL_AR_PL_WIP  Секция 3               C_EW(NS)8.4    490
# 12  AKD_GP4_ALL_AR_PL_WIP  Секция 4               C_EW(NS)8.4    524
# 10  AKD_GP4_ALL_AR_PL_WIP  Секция 5               C_CS(CN)5.8    698
# 3   AKD_GP4_ALL_AR_PL_WIP  Секция 6              С_CS(CN)7.10    821
# 25  AKD_GP4_ALL_AR_PL_WIP  Секция 7               C_EW(NS)6.4    366
# 15  AKD_GP4_ALL_AR_PL_WIP  Секция 8               C_EW(NS)8.4    461
# 29    DVT03_ALL_AR_CA_WIP  Секция 1              С_CS(CN)7.10    346
# 48    DVT03_ALL_AR_CA_WIP  Секция 2               C_EW(NS)6.4    156
# 24    DVT03_ALL_AR_CA_WIP  Секция 3              С_CS(CN)7.10    366
# 47    DVT03_ALL_AR_CA_WIP  Секция 4               C_EW(NS)8.4    180
# 37    DVT03_ALL_AR_CA_WIP  Секция 5               С_CS(CN)7.8    245
# 43    DVT03_ALL_AR_CA_WIP  Секция 6               C_EW(NS)8.4    190
# 38    DVT03_ALL_AR_CA_WIP  Секция 7               С_CS(CN)7.8    241
# 44    DVT03_ALL_AR_CA_WIP  Секция 8   C_EW(NS)8 не актуальная    189
# 53    DVT03_ALL_AR_CP_WIP   Паркинг  Parking1_(1500m2-2100m2)     49
# 4     DVT03_ALL_AR_CP_WIP  Секция 1              С_CS(CN)7.10    768
# 36    DVT03_ALL_AR_CP_WIP  Секция 2               C_EW(NS)6.4    255
# 9     DVT03_ALL_AR_CP_WIP  Секция 3              С_CS(CN)7.10    703
# 31    DVT03_ALL_AR_CP_WIP  Секция 4               C_EW(NS)8.4    322
# 13    DVT03_ALL_AR_CP_WIP  Секция 5               С_CS(CN)7.8    501
# 33    DVT03_ALL_AR_CP_WIP  Секция 6               C_EW(NS)8.4    310
# 18    DVT03_ALL_AR_CP_WIP  Секция 7               С_CS(CN)7.8    430
# 20    DVT03_ALL_AR_CP_WIP  Секция 8   C_EW(NS)8 не актуальная    397
# 32  MED_GP02_ALL_AR_FR_RD  Секция 1              B_CS(CN)7.10    319
# 50  MED_GP02_ALL_AR_FR_RD  Секция 2               B_EW(NS)6.4    148
# 49  MED_GP02_ALL_AR_FR_RD  Секция 4               B_EW(NS)6.4    152
# 40  MED_GP02_ALL_AR_FR_RD  Секция 5               B_CS(CN)5.8    201
# 42  MED_GP02_ALL_AR_FR_RD  Секция 6               B_EW(NS)8.4    194
# 45  MED_GP02_ALL_AR_FR_RD  Секция 7               B_CS(CN)5.8    187
# 51  MED_GP02_ALL_AR_FR_RD  Секция 8               B_EW(NS)6.4    138
# 55  MED_GP02_ALL_AR_PL_RD   Паркинг  Parking1_(1500m2-2100m2)      7
# 7   MED_GP02_ALL_AR_PL_RD  Секция 1              B_CS(CN)7.10    719
# 22  MED_GP02_ALL_AR_PL_RD  Секция 2               B_EW(NS)6.4    377
# 27  MED_GP02_ALL_AR_PL_RD  Секция 4               B_EW(NS)6.4    352
# 17  MED_GP02_ALL_AR_PL_RD  Секция 5               B_CS(CN)5.8    432
# 19  MED_GP02_ALL_AR_PL_RD  Секция 6               B_EW(NS)8.4    418
# 30  MED_GP02_ALL_AR_PL_RD  Секция 7               B_CS(CN)5.8    346
# 35  MED_GP02_ALL_AR_PL_RD  Секция 8               B_EW(NS)6.4    282
# 0     UKV_GP11_ALL_AR_SHD  Секция 1             B_50TS(TN)5.8   1324
# 2     UKV_GP11_ALL_AR_SHD  Секция 2               B_CS(CN)7.8    920
# 11    UKV_GP11_ALL_AR_SHD  Секция 3   B_EW(NS)8 не актуальная    572
# 5     UKV_GP11_ALL_AR_SHD  Секция 4               B_CS(CN)7.8    758
# 8     UKV_GP11_ALL_AR_SHD  Секция 5               B_CS(CN)5.8    706
# 16    UKV_GP11_ALL_AR_SHD  Секция 6   B_EW(NS)6 не актуальная    451
# 23    UKV_GP11_ALL_AR_SHD  Секция 7   B_EW(NS)6 не актуальная    376
res = dfFull[['name','Секция','Морфотип секции']].value_counts().reset_index().sort_values(by = ['name','Секция','Морфотип секции'])


#Проверка этажей - гуд
res = dfFull[['name','Секция','Этаж','Тип этажа']].value_counts().reset_index().sort_values(by = ['name','Секция','Этаж','Тип этажа'])

#Сколько элементов без секции
res = dfFull[dfFull['Секция'].isnull()]


#Сколько элементов без этажа
res = dfFull[dfFull['Секция'] == 'NaN']


#Собираем датафрейм по этажам секций - словарь с датафреймами по типу СК
res = volHel.get_df_array_by_floor_sum(dfFull,sk_arr,'Объем, м3')[sk_arr[0]]


#Проверка значений
#Морфотип - С_CS(CN)7.8
#Объект - DVT03_ALL_AR_CP_WIP
#Этаж - Этаж 05
#Объем = 4.211761
test_floor_sk_df = dfFull[(dfFull['Морфотип секции'] == 'B_50TS(TN)5.8')
             & (dfFull['name'] == 'UKV_GP11_ALL_AR_SHD')
             & (dfFull['Имя СК'] == sk_arr[0])
             & (dfFull['Этаж'] == 'Этаж 02')] [['Морфотип секции','name','Имя СК','Этаж','Объем, м3']]
# res = test_floor_sk_df['Объем, м3'].sum()




#endregion
#region Подсчет эталонов
standarts_df_arr = volHel.get_df_arr_sk_dev(dfFull,sk_arr,'Объем, м3')
#endregion
#region Графики

#Boxplot
# volHel.save_boxplotes_for_morph_and_floor_types(sk_arr,dfFull,'Кладка',plotsDir)

#endregion

test = standarts_df_arr[sk_arr[0]]
res = dfFull


# plt.show()
print(res)

