import numpy as np
import re
import sys

import pandas as pd



class ParamsAndFuns:
    #Основные параметры
    name_pn = 'Имя'
    bru_destination_pn = 'Назначение'
    type_pn = 'Вид помещения'
    bru_category_pn = 'Категория'
    adsk_premise_number = 'Номер квартиры'
    adsk_premise_part_number = 'Номер помещения квартиры'
    premise_part_number = 'Номер части помещения'
    section_str_pn = 'Номер секции'
    bru_section_int_pn = 'Секция число'
    bru_floor_int_pn = 'Этаж'
    adsk_index_int_pn = 'Индекс квартиры'
    adsk_position_int_pn = 'Позиция'
    bru_premise_number_pn = 'Номер помещения'

    #Типы
    adsk_type_pn = 'Тип квартиры'
    bru_type_pn = 'BRU_Тип квартиры'

    #Кол-во комнат
    rooms_count = 'Количество комнат'

    #Площади
    bru_premise_part_area_pn = 'Площадь помещения'
    bru_premise_full_area_pn = 'Площадь квартиры без коэффициентов'
    bru_premise_non_summer_area_pn = 'Площадь без летних помещений'
    bru_premise_summer_area_pn = 'Площадь летних'
    bru_premise_living_area_pn = 'Площадь квартиры жилая'
    bru_premise_common_area_pn = 'Площадь квартиры общая'

    #Характеристики вида окон
    on_three_and_more_sides = 'На три и более сторон'
    on_three_sides = "Торцевая (три стороны)"

    #Характеристики ЛП
    has_terrase_on_roof = 'Терраса на кровле'
    has_terrase_on_floor = 'Терраса на земле'
    has_balcony = 'Балкон'
    has_cold_loggia = 'Лоджия (холодная)'
    has_warm_loggia = 'Лоджия (теплая)'
    summer_area_names = ['Балкон', 'Лоджия', 'Лоджия (холодная)', 'Терраса на земле', 'Терраса']

    #Характеристики этажности ритейла
    retail_elev_list = [bru_premise_number_pn,name_pn,'Одноуровневое на 1 этаже', 'Одноуровневое на 2 этаже', 'Одноуровневое на -1 этаже'
                      ,'Многоуровневое -1 этаж до 30проц','Многоуровневое -1 этаж более 30проц','Многоуровневое 2 этаж до 50проц'
                      ,'Многоуровневое 2 этаж более 50проц']

    #Характеристики объекта окна
    premise_number_to_pn = 'Номер помещения To'
    premise_number_from_pn = 'Номер помещения From'
    premise_part_number_to_pn = 'Номер части помещения To'
    premise_part_number_from_pn = 'Номер части помещения From'
    premise_part_name_to_pn = 'Имя помещения To'
    premise_part_name_from_pn = 'Имя помещения From'
    orient_side_pn = 'Сторона света'
    # WindowDest = 'BRU_Направление'
    window_colums = [
        # 'Комментарии',
        bru_destination_pn,
        premise_number_to_pn,
        premise_part_number_to_pn,
        premise_number_from_pn,
        premise_part_number_from_pn,
        # PremisePartNameFromPN,
        # PremisePartNameToPN,
        orient_side_pn,
        # WindowDest
    ]

    @staticmethod
    def set_np_pd_opts():
        '''
        Установка отображения для np и pd. Колонок - 10, ширина 340
        Returns
        -------
        None
        '''
        desired_width = 340
        pd.set_option('display.width', desired_width)
        np.set_printoptions(linewidth=desired_width)
        pd.set_option('display.max_columns', 10)

    @staticmethod
    def convert_to_double(value):
        """
        Метод для запуска с функцией apply, превращает все числовые строки в float
        Parameters
        ----------
        value : object
            Значение которое нужно сконвертировать
        Returns
        -------
        floar or object
            Вернет float, если смог и object если не смог
        """
        try:
            return value.astype(float)
        except:
            return value

    @staticmethod
    def define_tech_equipment(roomName):
        """
        Маппинг имени тех помещения и оборудования, которое там должно стоять. По форме для РС
        Parameters
        ----------
        roomName : str
            Имя помещения (технического)
        Returns
        -------
        str
            Название оборудования
        """
        if ('Вент' in roomName):
            return 'Сети вентиляции'
        elif ('ИТП' in roomName):
            return 'Сети теплоснабжения'
        elif ('усор' in roomName):
            return 'Промежуточный пункт для вывоза мусора'
        elif ('асос' in roomName):
            return 'Система холодного хозяйственно-питьевого водоснабжения'
        elif ('Помещение СС' in roomName):
            return 'Сети связи'
        elif ('Помещение ТП' in roomName):
            return 'Сети теплоснабжения'
        elif ('Помещение трансформаторов' in roomName):
            return 'Сети электроснабжения'
        elif ('Электрощитовая' in roomName):
            return 'Сети электроснабжения'
        elif ('Вертикальный транспорт' in roomName):
            return 'Лифт'
        else:
            return ''

    @staticmethod
    def get_unique(someStr):
        """
        Метод берет строку со значениями через запятую и возвращает строку с перечислением УНИКАЛЬНЫХ значений
        Parameters
        ----------
        someStr : str
            Строка с перечислением через запятую
        Returns
        -------
        str
            Строка с перечислением уникальных значений через запятую
        """
        someList = str(someStr).split(sep=',')
        unique = list(set(someList))
        return ','.join(unique)

    @staticmethod
    def count_items(someStr):
        """
        Считает количество значений в строке, где идет перечисление элементов через запятую
        Parameters
        ----------
        someStr : str
            Значение элементов через запятую
        Returns
        -------
        int
            Количество
        """
        someList = str(someStr).split(sep=',')
        return len(someList)

    @staticmethod
    def show_columns_with_word(df,word):
        """
        Показывает список колонок в датафрейме, которые содержат нужное слово (вне зависимости от регистра)
        Parameters
        ----------
        df : pd.Dataframe
            Датафрейм
        word : str
            Ключевое слово для поиска
        Returns
        -------
        np.array
            Массив с названием колонок
        """
        res = df.columns[df.columns.str.contains(word,False) == True]
        return res

    @staticmethod
    def regex_floor_name(floor):
        """
        Достает значение - Этаж -01 из строки Этаж -01 (отм. -4.200)
        Parameters
        ----------
        floor : str
            Название уровня
        Returns
        -------
        str
            Имя уровня
        """
        if (floor is np.nan):
            return 'NaN'
        pattern = r'Этаж\s(-?\d+)'
        match = re.search(pattern, floor)
        if (match):
            res = match.group()
        else:
            res = 'NaN'
        return res

    @staticmethod
    def show_stat_info(df,start_t,fin_t):
        """
        Вывод сообщения для статистики с информацией об общем времени работы и весе датафрейма
        Parameters
        ----------
        df : pd.Dataframe
            Датафрейм
        start_t : time.time
            Начало работы
        fin_t : time.time
            Конец работы

        Returns
        -------
        None
            Сообщение в командную строку
        """
        full_time = fin_t - start_t
        mbite = round(sys.getsizeof(df) * (0.125*(10**(-6))),2)
        print(f'Затраченное время: {full_time}, вес: {mbite} МБайт')

    @staticmethod
    def load_few_df(dir,name_arr):
        """
        Метод для загрузки нескольких датафреймов и объединения их в один
        Parameters
        ----------
        dir : (str)
            Директория
        name_arr : []
            Список имен файлов в формате csv, разделитель ';'

        Returns
        -------
        pd.Dataframe
            Полный датафрейм
        """
        columns = pd.read_csv(dir+name_arr[0],sep=';').columns
        dfFull = pd.DataFrame(columns=columns)
        for name in name_arr:
            df = pd.read_csv(dir + name, sep=';')
            dfFull = pd.concat([dfFull, df], sort=False, axis=0)
        return dfFull

    @staticmethod
    def save_log(dfFull,short_name,dir):
        """
        Сохранение txt лога с информацией о собранном датафрейме СК
        Parameters
        ----------
        dfFull : pd.Dataframe
            Полный датафрейм
        short_name : (str)
            Короткое название для суффикса в логе (какие СК были собраны) - Кладка
        dir : (str)
            Директория

        Returns
        -------
        None
            Сохраняет лог в формате txt
        """
        log = '=====ИНФОРМАЦИЯ ДЛЯ ПРОВЕРКИ====='

        #Версии и количество
        log = log + '\nСостав моделей по версиям и количеству элементов\n'
        value = dfFull[['name', 'version_index']].value_counts()
        info = str(value)
        log = log + info

        #Секции и морфотипы
        log = log + '\n\nСекции и их морфотипы\n'
        value = dfFull[['name', 'Секция', 'Морфотип секции']].value_counts().reset_index().sort_values(
                by=['name', 'Секция', 'Морфотип секции'])
        info = str(value)
        log = log + info

        # Сколько элементов без секции
        null_sect_count = len(dfFull[dfFull['Секция'].isnull()])
        log = log + f'\n\nЭлементы без ADSK_Номер секции - {null_sect_count}\n'

        # Сколько элементов без этажа
        null_floor_count = len(dfFull[dfFull['Этаж'].isnull()])
        log = log + f'\n\nЭлементы без ADSK_Этаж - {null_floor_count}\n'

        # Незаполненные секции
        log = log + '\n\nНезаполненные секции\n'
        value = dfFull[dfFull['Морфотип секции'].isnull()]
        value = value[['name', 'Секция','Этаж']].value_counts().reset_index().sort_values(
            by=['name', 'Секция','Этаж'])
        info = str(value)
        log = log + info

        # Незаполненные этажи
        log = log + '\n\nНезаполненные этажи\n'
        value = dfFull[dfFull['Тип этажа'].isnull()]
        value = value[['name', 'Секция', 'Этаж']].value_counts().reset_index().sort_values(
            by=['name', 'Секция', 'Этаж'])
        info = str(value)
        log = log + info
        with open(dir + fr'\{short_name}_info.txt', "w") as file:
            file.write(log)



