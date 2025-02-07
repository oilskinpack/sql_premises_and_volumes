import seaborn as sns
from Access.AccessInfo import AccessInfo as ai
from Helpers.ParamsAndFuns import ParamsAndFuns as p
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from Helpers.DbConnector import DbConnector
from Helpers.VolumesHelper import VolumesHelper
p.set_np_pd_opts()


source_path = r'D:\Khabarov\Репозиторий\sql_premises_and_volumes\SourceData\ИсходныеДанные.xlsx' #Здесь укажи путь к табл
dbCon = DbConnector()
dfFull = dbCon.get_volumes_df(source_path=source_path)
volHel = VolumesHelper(dfFull)


