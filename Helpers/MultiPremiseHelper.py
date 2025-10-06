import pandas as pd
from Helpers.DbConnector import DbConnector
from Helpers.PremiseHelper import PremiseHelper
from Helpers.ParamsAndFuns import ParamsAndFuns as p
from datetime import datetime
import numpy as np


class MultiPremiseHelper:
    """Обновленный класс PremiseHelper, способный загружать данные по помещениям сразу по нескольким домам
    """

    def __init__(self,source_path: str,dbCon : DbConnector) -> None:
        """Конструктор класса

        Parameters
        ----------
        source_path
            Полный абсолютный путь до эксель таблицы ИсходныеДанные
        dbCon
            Созданный экземпляр DBConnector
        """
        self.dfFull = self.__load_full_df_premises(source_path,dbCon)
        self.dbCon = dbCon
    
    def __load_full_df_premises(self,source_path: str,dbCon: DbConnector) -> pd.DataFrame:
        """Загрузка полного датафрейма с помещениями

        Parameters
        ----------
        source_path
            Полный абсолютный путь до эксель таблицы ИсходныеДанные
        dbCon
            Созданный экземпляр DBConnector

        Returns
        -------
            _description_
        """
        data = None

        co_df_info = pd.read_excel(source_path,sheet_name='Объекты')[['name','Стадия','construction_object_id']].dropna(axis=0)
        for ind,co in co_df_info.iterrows():
                coId = co['construction_object_id']
                stage = co['Стадия']
                name = co['name']

                try:
                    df = dbCon.getFullDfPremise(coId, stage, 'premise',version=999)
                    premHel = PremiseHelper(df)
                    if(data is None):
                        data = premHel.fullDf
                        data['Наименование ОС'] = name
                        data = data
                    else:
                        data = premHel.fullDf
                        data['Наименование ОС'] = name
                        data = pd.concat([data,data],axis=0)
                    print(f'Добавлен {name} {stage}')
                except:
                    print(f'Не удалось загрузить {name} {stage}')
        return data
    
    def __get_living_etp_df(self,full_df: pd.DataFrame) -> pd.DataFrame:
        """Получение датафрейма с квартирами

        Parameters
        ----------
        full_df
            Полный датафрейм с помещениями

        Returns
        -------
            Датафрейм с квартирами
        """

        df = full_df

        #Берем помещения квартир
        premises = df[df[p.bru_destination_pn] == 'Жилье']
        premises = premises.copy()[[
                        p.adsk_premise_number
                    ,p.section_str_pn
                    ,p.rooms_sale_count
                    ,p.bru_premise_non_summer_area_pn
                    ,p.bru_premise_summer_area_pn
                    ,p.bru_premise_full_area_pn
                    ,p.bru_floor_int_pn
                    ,p.name_pn
                    ,p.bru_premise_part_area_pn
                    ,"Антресоль"
                    ,"Дуплекс"
                    ,"С цокольным этажом"
                    ,"Терраса на кровле"
                    ,"Терраса на земле"
                    ,"Летняя кухня на крыше"
                    ,"Второй свет"
                    ,"Отдельный вход"
                    ,"Пентхаус"
                    ,"Свободная планировка"
                    ]]

        #Определяем уникальность
        premises["Уникальность"] = (premises["Терраса на кровле"] 
                                + premises["Терраса на земле"]
                                + premises["Антресоль"]
                                + premises["Дуплекс"]
                                + premises["С цокольным этажом"]
                                + premises["Летняя кухня на крыше"]
                                + premises["Второй свет"]
                                + premises["Пентхаус"]
                                + premises["Свободная планировка"]
                                ) > 0
        premises['Уникальность'] = premises['Уникальность'].apply(lambda x: "Особенная" if x == True else "Стандартная")

        #Парсим основной этаж
        premises.loc[premises.index,'Секция'] = premises.loc[premises.index,p.adsk_premise_number].str.split('.').str[0]
        premises['Индекс квартиры'] = premises[p.adsk_premise_number].str.split('.').str[2]

        #Добавляем площади по лп для суммирования
        premises['Пл_балкона'] = np.where(premises[p.name_pn] == "Балкон", premises[p.bru_premise_part_area_pn],0)
        premises['Пл_лоджии'] = np.where(premises[p.name_pn] == "Лоджия", premises[p.bru_premise_part_area_pn],0)
        premises['Пл_лоджии_хол'] = np.where(premises[p.name_pn] == "Лоджия (холодная)", premises[p.bru_premise_part_area_pn],0)
        premises['Пл_лоджии_техн'] = np.where(premises[p.name_pn] == "Лоджия (техническая)", premises[p.bru_premise_part_area_pn],0)
        premises['Пл_хол_клад'] = np.where(premises[p.name_pn] == "Холодная кладовая", premises[p.bru_premise_part_area_pn],0)
        premises['Терраса_земля'] = np.where(premises[p.name_pn] == "Терраса на земле", premises[p.bru_premise_part_area_pn],0)
        premises['Терраса_кровля'] = np.where(premises[p.name_pn] == "Терраса", premises[p.bru_premise_part_area_pn],0)
        premises
        premises_gr = premises.groupby([p.adsk_premise_number,p.bru_floor_int_pn],as_index=False).agg(Номер_секции=('Секция','first')
                                                                                                ,Кол_во_комнат=(p.rooms_sale_count,'first')
                                                                                                ,Пл_без_ЛП=(p.bru_premise_non_summer_area_pn,'first')
                                                                                                ,Пл_ЛП=(p.bru_premise_summer_area_pn,'first')
                                                                                                ,Пл_без_Коэф=(p.bru_premise_full_area_pn,'first')
                                                                                                ,Уникальность=("Уникальность",'first')
                                                                                                ,Пл_уровня=(p.bru_premise_part_area_pn,'sum')
                                                                                                ,Пл_балкона=('Пл_балкона','sum')
                                                                                                ,Пл_лоджии=('Пл_лоджии','sum')
                                                                                                ,Пл_лоджии_хол=('Пл_лоджии_хол','sum')
                                                                                                ,Пл_лоджии_техн=('Пл_лоджии_техн','sum')
                                                                                                ,Пл_хол_клад=('Пл_хол_клад','sum')
                                                                                                ,Терраса_кровля=('Терраса_кровля','sum')
                                                                                                ,Терраса_земля=('Терраса_земля','sum')
                                                                                                ,Индекс_квартиры=(p.adsk_index_int_pn,'first')
                                                                                                )
        #Переименовываем колонки
        mapper = {
                    "Номер_секции":"Номер секции"
                    ,"Кол_во_комнат":"Кол-во комнат\nдля продаж"
                    ,"Пл_без_ЛП":"Площадь\nбез летних, м²"
                    ,"Пл_ЛП":"Площадь\nлетних, м²"
                    ,"Пл_без_Коэф": "Площадь квартиры\nбез коэффициентов,м²"
                    ,"Пл_уровня" : "Площадь\nуровня, м²"
                    ,"Пл_балкона" : "Площадь\nбалкона, м²"
                    ,"Пл_лоджии" : "Площадь\nлоджии, м²"
                    ,"Пл_лоджии_хол" : "Площадь\nлоджии (холодной), м²"
                    ,"Пл_лоджии_техн": "Площадь\nлоджии (технической), м²"
                    ,"Пл_хол_клад" : "Площадь\nхолодной кладовой, м²"
                    ,"Терраса_кровля" : "Площадь\nтеррасы на\nкровле, м²"
                    ,"Терраса_земля" : "Площадь\nтеррасы на\nземле, м²"
                }
        premises_gr = premises_gr.rename(mapper=mapper,axis=1)

        #Преобразование типа данных
        num_cols = premises_gr.select_dtypes(include=[float]).columns
        premises_gr[num_cols] = premises_gr[num_cols].round(2)
        premises_gr['Этаж'] = premises_gr['Этаж'].astype(int)
        premises_gr['Индекс_квартиры'] = premises_gr['Индекс_квартиры'].astype(int)

        #Сортировка
        premises_gr = premises_gr.sort_values(by=[p.section_str_pn,p.bru_floor_int_pn,"Индекс_квартиры"])
        premises_gr = premises_gr.drop(labels=['Индекс_квартиры'],axis=1)
        premises_gr['Кол-во квартир'] = 1

        premises_gr = premises_gr[[
            'Кол-во квартир'
           ,"Номер квартиры"
            ,"Номер секции"
            ,"Кол-во комнат\nдля продаж"
            ,"Площадь\nбез летних, м²"
            ,"Площадь\nлетних, м²"
            ,"Площадь квартиры\nбез коэффициентов,м²"
            ,"Уникальность"
            ,"Этаж"
            ,"Площадь\nуровня, м²"
            ,"Площадь\nбалкона, м²"
            ,"Площадь\nлоджии, м²"
            ,"Площадь\nлоджии (холодной), м²"
            ,"Площадь\nлоджии (технической), м²"
            ,"Площадь\nхолодной кладовой, м²"
            ,"Площадь\nтеррасы на\nкровле, м²"
            ,"Площадь\nтеррасы на\nземле, м²"
        ]]

        return premises_gr
    

    def __get_not_living_etp_df(self,full_df: pd.DataFrame) -> pd.DataFrame:
        """Получение датафрейма с нежилыми помещениями

        Parameters
        ----------
        full_df
            Полный датафрейм с помещениями

        Returns
        -------
            Датафрейм с нежилыми помещениями
        """
        df = full_df

        dests = ['Ритейл','Кладовки','Машино-место']
        linked_prems_col_name = "BRU_Связанные помещения"

        common_and_tech = []
        premises_st = []
        for dest in dests:
            #Берем помещения ритейлов
            if dest == "Машино-место":
                premises_st = df[(df[p.type_pn] == dest)].copy()
            else:
                premises_st = df[(df[p.bru_destination_pn] == dest)
                                    & (df[p.type_pn] != "МОП")
                                    ].copy()
                
            col_names = [
                        p.adsk_premise_number
                        ,p.section_str_pn
                        ,p.bru_destination_pn
                        ,p.bru_premise_full_area_pn
                        ,p.bru_floor_int_pn
                        ,p.name_pn
                        ,p.bru_premise_part_area_pn
                        ,"Высота потолка от пола"
                        ,linked_prems_col_name
                        ]
            if linked_prems_col_name not in premises_st.columns:
                premises_st[linked_prems_col_name] = ""
            
            premises = premises_st[col_names].copy()

            #Парсим основной этаж
            if dest != "Машино-место":
                premises.loc[premises.index,'Секция'] = premises.loc[premises.index,p.adsk_premise_number].str.split('.').str[0]
            else:
                premises['Секция'] = premises[p.section_str_pn]
            
            premises['Индекс квартиры'] = premises[p.adsk_premise_number].str.split('.').str[2]
            premises['Основной этаж'] = premises[p.adsk_premise_number].str.split('.').str[1]

            #Добавляем площади по лп для суммирования
            premises['Пл_балкона'] = np.where(premises[p.name_pn] == "Балкон", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии'] = np.where(premises[p.name_pn] == "Лоджия", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии_хол'] = np.where(premises[p.name_pn] == "Лоджия (холодная)", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии_техн'] = np.where(premises[p.name_pn] == "Лоджия (техническая)", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_хол_клад'] = np.where(premises[p.name_pn] == "Холодная кладовая", premises[p.bru_premise_part_area_pn],0)
            premises['Терраса_земля'] = np.where(premises[p.name_pn] == "Терраса на земле", premises[p.bru_premise_part_area_pn],0)
            premises['Терраса_кровля'] = np.where(premises[p.name_pn] == "Терраса", premises[p.bru_premise_part_area_pn],0)

            premises_gr = premises.groupby([p.adsk_premise_number,p.bru_destination_pn,p.bru_floor_int_pn],as_index=False).agg(Номер_секции=('Секция','first')
                                                                                                    ,Пл_без_Коэф=(p.bru_premise_full_area_pn,'first')
                                                                                                    ,Высота=("Высота потолка от пола",'median')
                                                                                                    ,Осн_этаж=("Основной этаж",'first')
                                                                                                    ,Пл_уровня=(p.bru_premise_part_area_pn,'sum')
                                                                                                    ,Пл_балкона=('Пл_балкона','sum')
                                                                                                    ,Пл_лоджии=('Пл_лоджии','sum')
                                                                                                    ,Пл_лоджии_хол=('Пл_лоджии_хол','sum')
                                                                                                    ,Пл_лоджии_техн=('Пл_лоджии_техн','sum')
                                                                                                    ,Пл_хол_клад=('Пл_хол_клад','sum')
                                                                                                    ,Терраса_кровля=('Терраса_кровля','sum')
                                                                                                    ,Терраса_земля=('Терраса_земля','sum')
                                                                                                    ,Связанные_помещения=(linked_prems_col_name,'first')
                                                                                                    ,Индекс_квартиры=(p.adsk_index_int_pn,'first')
                                                                                                    )

            #Переименовываем колонки
            mapper = {
                        "Номер_секции":"Номер секции"
                        ,"Номер квартиры": "Условный номер"
                        ,"Назначение": "Назначение\nпомещения"
                        ,"Осн_этаж": "Этаж\nрасположения"
                        ,"Пл_без_Коэф": "Площадь, м²"
                        ,"Высота": "Высота\nпотолков, м"
                        ,"Пл_уровня" : "Площадь\nуровня, м²"
                        ,"Пл_балкона" : "Площадь\nбалкона, м²"
                        ,"Пл_лоджии" : "Площадь\nлоджии, м²"
                        ,"Пл_лоджии_хол" : "Площадь\nлоджии (холодной), м²"
                        ,"Пл_лоджии_техн": "Площадь\nлоджии (технической), м²"
                        ,"Пл_хол_клад" : "Площадь\nхолодной кладовой, м²"
                        ,"Терраса_кровля" : "Площадь\nтеррасы на\nкровле, м²"
                        ,"Терраса_земля" : "Площадь\nтеррасы на\nземле, м²"
                        ,"Связанные_помещения": "Связанные\nпомещения"
                    }
            premises_gr = premises_gr.rename(mapper=mapper,axis=1)

            #Преобразование типа данных
            num_cols = premises_gr.select_dtypes(include=[float]).columns
            premises_gr[num_cols] = premises_gr[num_cols].round(2)
            premises_gr['Этаж\nрасположения'] = premises_gr['Этаж\nрасположения'].astype(int)
            premises_gr['Индекс_квартиры'] = premises_gr['Индекс_квартиры'].astype(int)

            #Сортировка
            premises_gr = premises_gr.sort_values(by=[p.section_str_pn,"Этаж\nрасположения","Индекс_квартиры"])
            premises_gr = premises_gr.drop(labels=['Индекс_квартиры'],axis=1)
            premises_gr

            if dest == 'Машино-место':
                premises_gr['Назначение\nпомещения'] = 'Машино-место'
                premises_gr['Площадь, м²'] = premises_gr['Площадь\nуровня, м²']
                premises_gr['Высота\nпотолков, м'] = 0
                premises_gr['Площадь\nуровня, м²'] = 0

            
            premises_gr = premises_gr[[
            'Условный номер'
           ,"Назначение\nпомещения"
            ,"Этаж\nрасположения"
            ,"Номер секции"
            ,"Площадь, м²"
            ,"Высота\nпотолков, м"
            ,"Этаж"
            ,"Площадь\nуровня, м²"
            ,"Площадь\nбалкона, м²"
            ,"Площадь\nлоджии, м²"
            ,"Площадь\nлоджии (холодной), м²"
            ,"Площадь\nлоджии (технической), м²"
            ,"Площадь\nхолодной кладовой, м²"
            ,"Площадь\nтеррасы на\nкровле, м²"
            ,"Площадь\nтеррасы на\nземле, м²"
            ,"Связанные\nпомещения"
            ]]
            
            common_and_tech.append(premises_gr)




        res = pd.concat(common_and_tech, axis=0, ignore_index=True)
        return res
    

    def __get_common_etp_df(self,full_df: pd.DataFrame) -> pd.DataFrame:
        """Получение датафрейма с моп и техн помещениями

        Parameters
        ----------
        full_df
            Полный датафрейм с помещениями

        Returns
        -------
            Датафрейм с моп и техн помещениями
        """

        df = full_df
        cats = ['Жилье','Кладовые','Коммерческие помещения','Паркинг',"Технические помещения"]

        common_and_tech = []

        for cat in cats:

            if cat != 'Технические помещения':
                premises = df[(df[p.bru_category_pn] == cat)
                                        & (df[p.type_pn] == "МОП")
                                        ]
            else:
                premises = df[(df[p.bru_category_pn] == cat)
                                ]
            premises = premises[[
                        p.name_pn
                        ,p.adsk_premise_number
                        ,p.bru_floor_int_pn
                        ,p.section_str_pn
                        ,p.type_pn
                        ,p.bru_premise_part_area_pn
                        ,p.bru_category_pn
                        ]]


            #Добавляем площади по лп для суммирования
            premises['Пл_балкона'] = np.where(premises[p.name_pn] == "Балкон", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии'] = np.where(premises[p.name_pn] == "Лоджия", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии_хол'] = np.where(premises[p.name_pn] == "Лоджия (холодная)", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_лоджии_техн'] = np.where(premises[p.name_pn] == "Лоджия (техническая)", premises[p.bru_premise_part_area_pn],0)
            premises['Пл_хол_клад'] = np.where(premises[p.name_pn] == "Холодная кладовая", premises[p.bru_premise_part_area_pn],0)
            premises['Терраса_кровля'] = np.where(premises[p.name_pn] == "Терраса", premises[p.bru_premise_part_area_pn],0)
            premises['Терраса_земля'] = np.where(premises[p.name_pn] == "Терраса на земле", premises[p.bru_premise_part_area_pn],0)

            #Парсим секцию
            premises['Секция'] = np.where(premises[p.section_str_pn].str.contains("Паркинг") == True
                                    ,premises[p.section_str_pn]
                                    ,premises[p.adsk_premise_number].str.split('.').str[0])
            
            premises = premises.drop(['Номер секции',p.adsk_premise_number],axis=1)

            #Переименовываем колонки
            mapper = {
                        "Этаж":"Этаж\nрасположения"
                        ,"Площадь помещения" : "Площадь, м²"
                        ,"Секция" : "Номер секции"
                        ,"Пл_балкона" : "Площадь\nбалкона, м²"
                        ,"Пл_лоджии" : "Площадь\nлоджии, м²"
                        ,"Пл_лоджии_хол" : "Площадь\nлоджии (холодной), м²"
                        ,"Пл_лоджии_техн" : "Площадь\nлоджии (технической), м²"
                        ,"Пл_хол_клад" : "Площадь\nхолодной кладовой, м²"
                        ,"Терраса_кровля" : "Площадь\nтеррасы на\nкровле, м²"
                        ,"Терраса_земля" : "Площадь\nтеррасы на\nземле, м²"
                    }
            premises = premises.rename(mapper=mapper,axis=1)


            #Преобразование типа данных
            num_cols = premises .select_dtypes(include=[float]).columns
            premises[num_cols] = premises[num_cols].round(2)
            premises['Этаж\nрасположения'] = premises['Этаж\nрасположения'].astype(int)
            premises["№ п/п"] = 1

            #Порядок колонок
            premises = premises[[
                            "№ п/п"
                            ,"Имя"
                            ,"Этаж\nрасположения"
                            ,"Номер секции"
                            ,"Вид помещения"
                            ,"Площадь, м²"
                            ,"Категория"
                            ,"Площадь\nбалкона, м²"
                            ,"Площадь\nлоджии, м²"
                            ,"Площадь\nлоджии (холодной), м²"
                            ,"Площадь\nлоджии (технической), м²"
                            ,"Площадь\nхолодной кладовой, м²"
                            ,"Площадь\nтеррасы на\nкровле, м²"
                            ,"Площадь\nтеррасы на\nземле, м²"
                        ]]

            #Сортировка
            premises = premises.sort_values(by=[p.section_str_pn,"Этаж\nрасположения"])

            common_and_tech.append(premises)

        #Объединяем в одну таблицу
        not_living_df = pd.concat(common_and_tech, axis=0, ignore_index=True)

        not_living_df = not_living_df.reset_index()
        not_living_df['№ п/п'] = not_living_df.index + 1
        not_living_df = not_living_df.drop(['index'],axis=1)
        


        return not_living_df
    
    def save_etp_form(self,directory: str) -> None:
        """Сохранение формы ЕТП

        Parameters
        ----------
        directory
            Абсолютный путь до директории, куда сохранять
        """
        data = self.dfFull
        co_name = data.iloc[0]['Наименование ОС']
        date = datetime.now().strftime("%d_%m_%Y_%H_%M")

        try:
            flats = self.__get_living_etp_df(data)
            not_living = self.__get_not_living_etp_df(data)
            common = self.__get_common_etp_df(data)
        except Exception as e:
            print(f"Не удалось выгрузить данные\nОписание ошибки\n:{e}")

        file_name = f"\{co_name}_Ведомость_для_ЕТП_{date}.xlsx"
        path = directory + file_name
        try:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                flats.to_excel(writer, sheet_name='Жилые', index=False)
                not_living .to_excel(writer, sheet_name='Нежилые', index=False)
                common.to_excel(writer, sheet_name='МОП', index=False)
            print("Файл ЕТП успешно сохранен!")
        except Exception as e:
            print(f"Ошибка при сохранении: {e}")

    def show_new_premises_for_dictionary(self) -> pd.DataFrame:
        """Показывает какие помещения из квартирографии не занесены в справочник помещений

        Returns
        -------
            Датафрейм с новыми помещениями
        """
        col_map = {'premise_category': p.bru_category_pn
           , 'premise_type': p.type_pn
           , 'premise_purpose': p.bru_destination_pn
           , 'premise_part_name': p.name_pn}

        myQuery = 'SELECT * FROM dict.premise_part_type_j_model_params'
        df_premises = (pd.read_sql_query(myQuery, con=self.dbCon.engine)
                    .rename(mapper=col_map,axis=1)
                    [[p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn,'premise_type_id','premise_part_type_id']]
                    .sort_values(by=[p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn]))

        unique_prems_os = self.dfFull.groupby([p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn],as_index=False).first()[[p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn]]
        mapping_df = pd.merge(left=unique_prems_os,right=df_premises,how='left',on=[p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn])
        new_prems = mapping_df[mapping_df["premise_part_type_id"].isna()]
        # new_prems[[p.bru_category_pn,p.type_pn,p.bru_destination_pn,p.name_pn]].to_excel('D:\загрузки\МОНС01.xlsx',sheet_name='Лист1',index=False)

        return new_prems