from Helpers.VolumesHelper import VolumesHelper
from Helpers.DbConnector import DbConnector
from Helpers.MultiPremiseHelper import MultiPremiseHelper
import pandas as pd
from Helpers.ParamsAndFuns import ParamsAndFuns as p


class ComplexCOHelper:

    def __init__(self,volume_source_path,premise_source_path):
        #Загрузка данных по СК
        volHel = VolumesHelper(source_path=volume_source_path)
        self.volume_helper = volHel
        self.df_volumes = volHel.fullDf

        #Загрузка данных по помещениям
        dbCon = DbConnector()
        self.multi_prems = MultiPremiseHelper(premise_source_path,dbCon)
        self.df_premises = self.multi_prems.dfFull
        print('Данные по помещениям и СК загружены в экземпляр')
    
    def get_floor_types_features_dataset(self):
        df_prems = self.multi_prems.get_floor_types_features_by_premises()
        df_volumes = self.volume_helper.get_floor_types_features_by_columns()

        df_volumes = p.expand_sections(df_volumes,'Номер секции')
        
        df = pd.merge(df_prems,df_volumes,how='left',on=['Наименование ОС','Номер секции','Этаж'])
        not_mapped = df[df['pylon_height'].isna()]
        print(f'Всего строк: {len(df)}, не удалось сопоставить: {len(not_mapped)}')

        return df

