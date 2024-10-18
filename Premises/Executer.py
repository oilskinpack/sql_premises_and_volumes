import os
from itertools import groupby

import numpy as np
import pandas as pd
from Helpers.DbConnector import DbConnector
from Helpers.PremiseHelper import PremiseHelper
from Helpers.ParamsAndFuns import ParamsAndFuns as p
from Access.AccessInfo import AccessInfo as ai

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)
res = ''

# region Параметры подключения
host = ai.Host
database = ai.Database
user = ai.User
password = ai.Password

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
    dbCon = DbConnector(host, user, password, database)
    #Подгружаем датафрейм по помещениям и сохраням
    premiseParamsValues = dbCon.getFullDfPremise(coId, stage, modelType,version=version)
    premiseParamsValues.to_csv(full_path, sep=';')

#endregion


premHel = PremiseHelper(full_path)
# res = premHel.getDfOfSellPremisesByDest('Ритейл')
# res = str(res[res[p.NamePN].str.contains('КУИ+',False) == True].reset_index().iloc[0][p.NamePN])
res = premHel.getFlatTypeMatrix(False)


print(res)
