import os
from operator import index
from tokenize import group

import numpy as np
import pandas as pd


from Helpers.DbConnector import DbConnector
from Helpers.PremiseHelper import PremiseHelper
from Helpers.ParamsAndFuns import ParamsAndFuns as p
from Access.AccessInfo import AccessInfo as ai
from Volumes.Tester import dfFull

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)
res = ''

# region Параметры подключения
coId= ai.co_id_prem
stage = ai.stage_prem
modelType = 'premise'
shortName = ai.short_name_prem
version = ai.version_prem
full_path = ai.full_path_prem
#endregion
#region Получаем датафрейм и сохраняем в csv
if os.path.exists(full_path):
    pass
else:
    # Создаем экземпляр для выгрузки из БД
    # source_path = r'D:\Khabarov\Репозиторий\sql_premises_and_volumes\SourceData\ИсходныеДанные.xlsx'
    dbCon = DbConnector()
    #Подгружаем датафрейм по помещениям и сохраням
    premiseParamsValues = dbCon.getFullDfPremise(coId, stage, modelType,version=version)
    premiseParamsValues.to_csv(full_path, sep=';')

#endregion

premHel = PremiseHelper(full_path)

#Исходные данные
crm_path = r'D:\Khabarov\RVT\ШГР16\18.02\ШГ16_Шахматка.xlsx'



res = premHel.get_comparing_df_crm_bim(crm_path=crm_path,how='left')
res.to_excel(r'D:\Khabarov\RVT\ШГР16\18.02\ШГ16_СравнениеСРМBIM.xlsx',sheet_name='Лист1',index=False)
# res = premHel.show_premise_info('Н2.2.6',[p.name_pn,p.bru_section_int_pn,p.bru_floor_int_pn,p.adsk_index_int_pn])

# flats = premHel.getDfOfSellPremisesByDest('Жилье')
# res = flats[(flats[p.adsk_type_pn].isin(['СП','СД'])) & (flats[p.rooms_count] == 3)]
# res = res.groupby(p.adsk_premise_number).apply(lambda x: x.nlargest(1,p.bru_premise_part_area_pn))
# res = res[p.bru_premise_non_summer_area_pn].sum()

print(res)
