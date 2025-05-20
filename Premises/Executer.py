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
flats = premHel.getDfOfSellPremisesByDestGrouped('Жилье')
oneb = flats[flats[p.rooms_sale_count] == 1]

uniq_features = [p.has_terrase_on_roof
                ,p.has_terrase_on_floor
                ,p.has_duplex
                ,p.has_mezzanine
                ,p.has_groundfloor
                # ,p.on_four_sides
                 ,p.second_light
                 ,p.separate_enter
                 ,p.penthouse
                 ,p.free_plane
                ,p.has_summer_kitchen]

res = p.show_columns_with_word(oneb,'Своб')
oneb = oneb[uniq_features]
res =  oneb[oneb.sum(axis=1) > 0]


pantries = premHel.getDfOfSellPremisesByDest('Кладовки')
park_pans = pantries[pantries[p.bru_section_int_pn] == 4]
res = park_pans[[p.adsk_premise_number,p.section_str_pn,p.type_pn]]


print(res)
