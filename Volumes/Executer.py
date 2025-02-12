from Helpers.ParamsAndFuns import ParamsAndFuns as p
import numpy as np
import pandas as pd
from Helpers.VolumesHelper import VolumesHelper
p.set_np_pd_opts()


source_path = r'D:\Khabarov\Репозиторий\sql_premises_and_volumes\SourceData\ИсходныеДанные.xlsx' #Здесь укажи путь к табл
volHel = VolumesHelper(source_path=source_path)
dfFull = volHel.fullDf

#Выгрузка номенклатуры
directory = r'D:\Khabarov\Репозиторий\sql_premises_and_volumes\Data'
volHel.save_nomenclature(source_path=source_path,directory=directory)

print(dfFull.isna().sum())



