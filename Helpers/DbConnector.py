import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from Helpers.ParamsAndFuns import ParamsAndFuns as p
import time

class DbConnector():
    """
    Класс для создания подключения к БД

    Attributes
    -----------
    engine: _engine.Engine
        Экземпляр подключения

    Methods
    --------
    createEngine():
        Создает экземпляр подключения

    """

    def __init__(self,host,user,password,database):
        self.engine = create_engine(f"postgresql://{user}:{password}@{host}/{database}")
        print('Подключено')

    def getFullDfPremise(self,coId,stage,modelType,version = 999):

        # Стадии
        #  #   Column             Non-Null Count  Dtype
        # ---  ------             --------------  -----
        #  0   stage_id           3 non-null      object
        #  1   title              3 non-null      object
        #  2   codifier           3 non-null      object
        #  3   model_stage_order  3 non-null      int64
        myQuery = 'SELECT * FROM bim.stages'
        dfStages = pd.read_sql_query(myQuery, con=self.engine)

        # Стадии модели
        #  #   Column                  Non-Null Count  Dtype
        # ---  ------                  --------------  -----
        #  0   model_stage_id          156 non-null    object
        #  1   stage_id                156 non-null    object
        #  2   construction_object_id  156 non-null    object
        myQuery = 'SELECT * FROM bim.model_stages'
        dfModelStages = pd.read_sql_query(myQuery, con=self.engine)
        # Находим нужный ОС
        dfModelStages = dfModelStages[dfModelStages['construction_object_id'].astype(str) == coId]

        # Стадия + объект
        dfFull = pd.merge(left=dfModelStages, right=dfStages, how='left', on='stage_id')
        dfFull = dfFull[['model_stage_id', 'stage_id', 'construction_object_id', 'title']]
        # Берем нужную стадию
        dfFull = dfFull[dfFull['title'] == stage]

        # Модели
        #  #   Column          Non-Null Count  Dtype
        # ---  ------          --------------  -----
        #  0   model_id        618 non-null    object
        #  1   model_stage_id  618 non-null    object
        #  2   company_id      618 non-null    object
        #  3   name            618 non-null    object
        #  4   model_type      618 non-null    object
        #  5   sw              618 non-null    object
        #  6   upload_type     618 non-null    object
        #  7   created_at      618 non-null    datetime64[ns, UTC]
        #  8   is_archived     35 non-null     object
        #  9   archived_at     35 non-null     datetime64[ns, UTC]
        #  10  is_deleted      618 non-null    bool
        #  11  deleted_at      0 non-null      object
        #  12  created_by      0 non-null      object
        #  13  archived_by     35 non-null     object
        myQuery = 'SELECT * FROM bim.models'
        dfModels = pd.read_sql_query(myQuery, con=self.engine)

        # Модель + стадия модели
        dfFull = pd.merge(left=dfFull, right=dfModels, how='left', on='model_stage_id')
        dfFull = dfFull[['model_id', 'model_stage_id', 'stage_id', 'construction_object_id', 'title', 'name', 'model_type']]
        # Забираем только помещения
        dfFull = dfFull[dfFull['model_type'] == modelType]

        # Версии модели
        #  #   Column                Non-Null Count  Dtype
        # ---  ------                --------------  -----
        #  0   model_version_id      7290 non-null   object
        #  1   model_id              7290 non-null   object
        #  2   version_index         7290 non-null   int64
        #  3   model_path            7290 non-null   object
        #  4   total_elements_count  7290 non-null   int64
        #  5   elements_count        7290 non-null   int64
        #  6   description           7290 non-null   object
        #  7   created_at            7290 non-null   datetime64[ns, UTC]
        #  8   updated_at            46 non-null     datetime64[ns, UTC]
        #  9   created_by            0 non-null      object
        #  10  updated_by            0 non-null      object
        myQuery = 'SELECT * FROM bim.model_versions'
        dfModelVersions = pd.read_sql_query(myQuery, con=self.engine)

        # Версия + модель(Берется крайняя модель)
        dfFull = pd.merge(left=dfFull, right=dfModelVersions, how='left', on='model_id')

        if version == 999:
            dfFull = dfFull[dfFull['version_index'] == dfFull['version_index'].max()]
        else:
            dfFull = dfFull[dfFull['version_index'] == version]

        # Айди расчета
        myQuery = 'SELECT * FROM calcp.calcs'
        dfCalcs = pd.read_sql_query(myQuery, con=self.engine)

        # Ищем расчет для нужной версии объекта
        dfFull = pd.merge(left=dfFull, right=dfCalcs, how='left', on='model_version_id')
        calcId = dfFull.loc[0]['calc_id']

        # Расчет параметров конкретного расчета
        #  #   Column                    Non-Null Count   Dtype
        # ---  ------                    --------------   -----
        #  0   calcs_j_param_id          327120 non-null  object
        #  1   calc_id                   327120 non-null  object
        #  2   model_version_element_id  327120 non-null  object
        #  3   param_id                  327120 non-null  object
        #  4   value                     327120 non-null  object
        # dtypes: object(5)
        # memory usage: 12.5+ MB
        myQuery = f'SELECT * FROM calcp.calcs_j_param WHERE calc_id = \'{calcId}\''
        dfCalcsParamValues = pd.read_sql_query(myQuery, con=self.engine)

        # Имена параметров
        myQuery = 'SELECT * FROM prem.params'
        dfParamNames = pd.read_sql_query(myQuery, con=self.engine)

        # Значения параметров + имена параметров
        dfFull = pd.merge(left=dfCalcsParamValues, right=dfParamNames, how='left', on='param_id')

        return dfFull

    def get_volumes_df(self,co_id,stage_name,sk_arr):
        dfFull = []
        df_calc_ids = []
        calc_ids_arr = []
        start_time = time.time()
        # Стадии
        proper_stage_id = self.get_stage_df(stage_name)
        # Id нужной стадии нужного объекта строительства
        proper_model_stage_id = self.get_model_stages_for_objects(proper_stage_id,co_id)
        if(proper_model_stage_id is np.nan):
            print('Не найдена запрашиваемая стадия для объектов')
            return np.nan

        # Модели
        df_models = self.get_models_versions_full_df(proper_model_stage_id)
        if (df_models is np.nan):
            print('Не найдена запрашиваемая стадия для одного из объектов')
            return np.nan


        proper_model_ids_str = self.get_model_ids_str(df_models)

        # Версии модели
        myQuery = f"SELECT * FROM bim.model_versions WHERE model_id IN {proper_model_ids_str}"
        dfModelVersions = pd.read_sql_query(myQuery, con=self.engine)
        # Версия + модель
        dfFull = pd.merge(left=df_models, right=dfModelVersions, how='left', on='model_id')
        #Список крайних версий по всем моделям
        proper_model_versions = self.get_last_versions(proper_model_ids_str,df_models)
        # Датафрейм крайних id расчетов
        df_calcs = self.get_calcs_df_by_model_version_ids(proper_model_versions)


        # Ищем расчет для нужной версии объекта
        dfFull = pd.merge(left=df_calcs, right=dfFull, how='left', on='model_version_id')
        # Список всех calc_id которые нужны
        df_calc_ids = dfFull[['calc_id','name', 'version_index', 'model_version_id', 'model_id',
                              'created_at', 'recognized', 'elements_count', 'total_elements_count']]
        calc_ids_arr = str(tuple(df_calc_ids.astype(str)['calc_id'].array))

        # =====Распознавание=====
        # Параметры распознавания - берутся только нужные расчеты и нужные СК
        df_recogn_param_values = self.get_calc_recogn_params(calc_ids_arr,sk_arr)
        if(len(df_recogn_param_values) == 0):
            print("Ни одна из запрошенных типов СК не найдена в текущих объектах текущей стадии")
            return np.nan
        # Все айдишники элементов расчета, которые нужны
        all_model_vers_elem_ids = str(tuple(df_recogn_param_values['model_version_element_id'].astype(str).array))

        #=====Значения параметров=====
        # Значения параметров распознавания
        df_recogn_param_values = self.get_recogn_param_values(df_recogn_param_values)
        # Значения параметров стандартизации
        df_calc_standart_param = self.get_standard_param_values(calc_ids_arr,all_model_vers_elem_ids)
        # Значения параметров расчета
        df_calc_calculation_param = self.get_calculation_param_values(calc_ids_arr,all_model_vers_elem_ids)
        # Значения параметров расположения
        df_calc_location_param = self.get_location_param_values(calc_ids_arr,all_model_vers_elem_ids)
        # Объединяем датафреймы с параметрами
        dfFull = pd.concat([df_recogn_param_values, df_calc_standart_param], sort=False, axis=0)
        dfFull = pd.concat([dfFull, df_calc_calculation_param], sort=False, axis=0)
        dfFull = pd.concat([dfFull, df_calc_location_param], sort=False, axis=0)

        # Получаем датафрейм с информацией по объектам
        df_all_model_version_elem_id = dfFull[['calc_id', 'model_version_element_id']].value_counts().reset_index()[
            ['calc_id', 'model_version_element_id']]
        df_calc_ids_with_name = df_calc_ids[['calc_id','name','model_id','version_index']]
        df_model_info = pd.merge(left=df_all_model_version_elem_id, right=df_calc_ids_with_name, how='left',
                                 on='calc_id')

        # Транспонируем
        dfFull = pd.pivot_table(data=dfFull, index='model_version_element_id', columns='title', values='value',
                                aggfunc='first')
        #Добавляем колонки с информацией об объекте строительства и версии
        dfFull = pd.merge(left=dfFull, right=df_model_info, how='left', on='model_version_element_id')

        #Добавляем секции
        dfFull = self.add_section_info_to_df(stage_name, co_id, dfFull)

        #Время
        fin_time = time.time()
        p.show_stat_info(df=dfFull,start_t=start_time,fin_t=fin_time)

        return dfFull

    def get_stage_df(self,stage_name):
        # Стадии
        myQuery = 'SELECT * FROM bim.stages'
        dfStages = pd.read_sql_query(myQuery, con=self.engine)
        dfStages = dfStages[dfStages['title'] == stage_name]
        proper_stage_id = str(dfStages.iloc[0]['stage_id'])
        return proper_stage_id

    def get_model_stages_for_objects(self,stage_id,co_ids):
        # Стадии модели
        if (type(co_ids) is str):
            myQuery = f"SELECT * FROM bim.model_stages WHERE stage_id = '{stage_id}' AND construction_object_id = '{co_ids}'"
            dfModelStages = pd.read_sql_query(myQuery, con=self.engine)

            if(len(dfModelStages) == 0):
                return np.nan
            proper_model_stage_id = str(dfModelStages.iloc[0]['model_stage_id'])
        elif (type(co_ids) is tuple):
            myQuery = f"SELECT * FROM bim.model_stages WHERE stage_id = '{stage_id}' AND construction_object_id IN {str(co_ids)}"
            dfModelStages = pd.read_sql_query(myQuery, con=self.engine)

            if (len(dfModelStages) == 0):
                return np.nan
            proper_model_stage_id = str(tuple(dfModelStages.astype(str)['model_stage_id'].array))
        return proper_model_stage_id

    def get_model_ids_str(self,df_models):
        # Модели
        # dfModels = self.get_models_versions_full_df(model_stage_id)
        proper_model_id = df_models['model_id'].astype(str).array
        proper_model_ids = str(tuple(proper_model_id))
        return proper_model_ids

    def get_models_versions_full_df(self,model_stage_id):
        if (',' in model_stage_id):
            # Модели
            myQuery = f"SELECT * FROM bim.models WHERE model_stage_id IN {model_stage_id} AND model_type = 'volumes'"
            try:
                df_models = pd.read_sql_query(myQuery, con=self.engine)
            except:
                return np.nan
        else:
            # Модели
            myQuery = f"SELECT * FROM bim.models WHERE model_stage_id = '{model_stage_id}' AND model_type = 'volumes'"
            df_models = pd.read_sql_query(myQuery, con=self.engine)
        return df_models

    def get_last_versions(self,model_ids_str,models_version_full_df):
        # Версии модели
        myQuery = f"SELECT * FROM bim.model_versions WHERE model_id IN {model_ids_str}"
        dfModelVersions = pd.read_sql_query(myQuery, con=self.engine)

        # Версия + модель(Берется крайняя модель)
        dfFull = pd.merge(left=models_version_full_df, right=dfModelVersions, how='left', on='model_id')
        df_proper_model_versions = \
            dfFull.groupby('name', group_keys=False).apply(lambda x: x.nlargest(1, "version_index"))[
                ['name','version_index', 'model_version_id']]
        proper_model_versions_str = str(tuple(df_proper_model_versions.astype(str)['model_version_id'].array))
        return proper_model_versions_str

    def get_calcs_df_by_model_version_ids(self,model_versions):
        # Айди расчета
        myQuery = f"SELECT * FROM calc.calcs WHERE model_version_id IN {model_versions}"
        df_calcs = pd.read_sql_query(myQuery, con=self.engine)
        return df_calcs

    def get_calc_recogn_params(self,calc_ids_arr,sk_arr):
        # =====Распознавание=====
        # Параметры распознавания - берутся только нужные расчеты и нужные СК
        myQuery = f"SELECT * FROM calc.calcs_j_param_recognition WHERE calc_id IN {calc_ids_arr} AND value IN {str(sk_arr)}"
        df_calc_recogn_param = pd.read_sql_query(myQuery, con=self.engine)
        return df_calc_recogn_param

    def get_recogn_param_values(self,df_calc_recogn_params):
        # Названия параметров
        myQuery = f"SELECT * FROM param.recognition"
        df_recognition_ids = pd.read_sql_query(myQuery, con=self.engine)

        # Добавляем название параметров распознавания
        df_calc_recogn_param = pd.merge(left=df_calc_recogn_params, right=df_recognition_ids, how='left',
                                        on='recognition_id')
        df_calc_recogn_param = df_calc_recogn_param[['calc_id', 'model_version_element_id', 'value', 'title']]
        return df_calc_recogn_param

    def get_standard_param_values(self,calc_ids_arr,all_model_vers_elem_ids):
        # =====Стандартизация=====
        # Параметры стандартизации
        myQuery = f"SELECT * FROM calc.calcs_j_param_standard WHERE calc_id IN {calc_ids_arr} AND model_version_element_id IN {all_model_vers_elem_ids}"
        df_calc_standard_param = pd.read_sql_query(myQuery, con=self.engine)

        # Названия параметров
        myQuery = f"SELECT * FROM param.standard"
        df_standard_ids = pd.read_sql_query(myQuery, con=self.engine)

        # Добавляем название параметров стандартизации
        df_calc_standard_param = pd.merge(left=df_calc_standard_param, right=df_standard_ids, how='left',
                                          on='standard_id')
        df_calc_standard_param = df_calc_standard_param[['calc_id', 'model_version_element_id', 'value', 'title']]
        return df_calc_standard_param

    def get_calculation_param_values(self,calc_ids_arr,all_model_vers_elem_ids):
        # =====Расчет=====
        # Параметры расчета
        myQuery = f"SELECT * FROM calc.calcs_j_param_calculation WHERE calc_id IN {calc_ids_arr} AND model_version_element_id IN {all_model_vers_elem_ids}"
        df_calc_calculation_param = pd.read_sql_query(myQuery, con=self.engine)

        # Названия параметров
        myQuery = f"SELECT * FROM param.calculation"
        df_calculation_ids = pd.read_sql_query(myQuery, con=self.engine)

        # Добавляем название параметров расчета
        df_calc_calculation_param = pd.merge(left=df_calc_calculation_param, right=df_calculation_ids, how='left',
                                             on='calculation_id')
        df_calc_calculation_param = df_calc_calculation_param[['calc_id', 'model_version_element_id', 'value', 'title']]
        return df_calc_calculation_param

    def get_location_param_values(self,calc_ids_arr,all_model_vers_elem_ids):
        # =====Локация=====
        # Параметры расчета
        myQuery = f"SELECT * FROM calc.calcs_j_param_location WHERE calc_id IN {calc_ids_arr} AND model_version_element_id IN {all_model_vers_elem_ids}"
        df_calc_location_param = pd.read_sql_query(myQuery, con=self.engine)

        # Названия параметров
        myQuery = f"SELECT * FROM param.location"
        df_location_ids = pd.read_sql_query(myQuery, con=self.engine)

        # Добавляем название параметров расчета
        df_calc_location_param = pd.merge(left=df_calc_location_param, right=df_location_ids, how='left',
                                          on='location_id')
        df_calc_location_param = df_calc_location_param[['calc_id', 'model_version_element_id', 'value', 'title']]
        return df_calc_location_param

    def get_section_df_info(self,coId):
        # Секции для нужных объектов
        if (type(coId) is str):
            myQuery = f"SELECT * FROM sections.construction_object_sections WHERE construction_object_id = '{coId}'"
            df_sections = pd.read_sql_query(myQuery, con=self.engine)
        elif (type(coId) is tuple):
            myQuery = f"SELECT * FROM sections.construction_object_sections WHERE construction_object_id IN {str(coId)}"
            df_sections = pd.read_sql_query(myQuery, con=self.engine)

        section_type_ids = df_sections['section_type_id'].astype(str).array
        section_type_str = str(tuple(section_type_ids))

        myQuery = f"SELECT * FROM dict.section_types WHERE section_type_id IN {section_type_str}"
        df_types = pd.read_sql_query(myQuery, con=self.engine)

        df_sect_info = pd.merge(left=df_sections, right=df_types, how='left', on='section_type_id')[
            ['construction_object_id','construction_object_section_id','title_x', 'title_y', 'is_parking']]
        df_sect_info = df_sect_info.rename(columns={'title_x':'Секция','title_y':'Морфотип секции','is_parking':'Секция паркинг'})
        return df_sect_info

    def add_section_info_to_df(self, stage, coId, df_full):
        proper_stage_id = self.get_stage_df(stage)
        # Id нужной стадии нужного объекта строительства
        proper_model_stage_id = self.get_model_stages_for_objects(proper_stage_id, coId)
        if (proper_model_stage_id is np.nan):
            print('Не найдена запрашиваемая стадия для объектов')

        # Модели
        df_models = self.get_models_versions_full_df(proper_model_stage_id)[['model_id', 'model_stage_id']]

        proper_stage_id = self.get_stage_df(stage)
        if (type(coId) is str):
            myQuery = f"SELECT * FROM bim.model_stages WHERE stage_id = '{proper_stage_id}' AND construction_object_id = '{coId}'"
            dfModelStages = pd.read_sql_query(myQuery, con=self.engine)
        elif (type(coId) is tuple):
            myQuery = f"SELECT * FROM bim.model_stages WHERE stage_id = '{proper_stage_id}' AND construction_object_id IN {str(coId)}"
            dfModelStages = pd.read_sql_query(myQuery, con=self.engine)

        df_constr_id_info = pd.merge(left=df_models, right=dfModelStages, how='left', on='model_stage_id')[
            ['model_id', 'construction_object_id']]
        df_full['model_id'] = df_full['model_id'].astype(str)
        df_constr_id_info['model_id'] = df_constr_id_info['model_id'].astype(str)
        df_full = pd.merge(left=df_full, right=df_constr_id_info, how='left', on='model_id')

        df_section_info = self.get_section_df_info(coId)
        df_floors_info = self.get_floor_df_info(coId)

        #Преобразование этажа
        df_full['Этаж'] = df_full['Этаж'].apply(p.regex_floor_name)

        df_full = pd.merge(left=df_full, right=df_section_info, how='left', on=['construction_object_id', 'Секция'])
        df_full = pd.merge(left=df_full, right=df_floors_info, how='left', on=['construction_object_section_id', 'Этаж'])
        df_full['model_id'] = df_full['model_id'].astype(str)
        return df_full

    def get_floor_df_info(self,coId):
        # Этажи для нужных объектов
        constr_object_sect_ids = tuple(
            self.get_section_df_info(coId)['construction_object_section_id'].astype(str).array)
        myQuery = f"SELECT * FROM sections.construction_object_floors WHERE construction_object_section_id IN {constr_object_sect_ids}"
        df_floors = pd.read_sql_query(myQuery, con=self.engine)
        floor_type_ids = df_floors['floor_type_id'].astype(str).array
        section_type_str = str(tuple(floor_type_ids))

        myQuery = f"SELECT * FROM dict.floor_types WHERE floor_type_id IN {section_type_str}"
        df_types = pd.read_sql_query(myQuery, con=self.engine)

        df_floor_info = pd.merge(left=df_floors, right=df_types, how='left', on='floor_type_id')[
            ['construction_object_section_id', 'title_x', 'title_y', 'is_parking']]
        df_floor_info = df_floor_info.rename(
            columns={'title_x': 'Этаж', 'title_y': 'Тип этажа', 'is_parking': 'Этаж паркинга'})
        return df_floor_info
