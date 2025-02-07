import pandas as pd
from Helpers.ParamsAndFuns import ParamsAndFuns as p
import seaborn as sns
import matplotlib.pyplot as plt
from Access.AccessInfo import AccessInfo as ai


class VolumesHelper:
    def __init__(self,fullPath):
        """
        Класс для загрузки датафрейма и работы с ним

        Parameters
        -------
        fullPath: str
            Путь к файлу формата r'D:\Khabarov\RVT\Premises\TEP'\test.csv, разделитель ';'
        """
        # Конвертация данных
        fullDf = pd.read_csv(fullPath, sep=';')
        self.fullDf = fullDf.apply(p.convert_to_double)

    def save_boxplotes_for_morph_and_floor_types(self,sk_df,df_full,pref_name,dir):
        """
        Метод для сохранения swarmplot графиков по всем СК, всем морфотипам и этажам
        Parameters
        ----------
        sk_arr : []
            Список СК
        df_full : pd.Dataframe
            Полный датафрейм с СК
        pref_name : (str)
            Короткое название описывающая СК - кладка
        dir : (str)
            Директория сохранения

        Returns
        -------
        None
            Сохранение графиков
        """
        sk_array = (tuple(sk_df['Имя СК'].array))
        for sk in sk_array:
            param_n = sk_df[sk_df['Имя СК'] == sk]['Имя параметра'].iloc[0]
            one_sk_df = df_full[df_full['Имя СК'] == sk]
            volumes_df = one_sk_df.groupby(['Морфотип секции','Секция','construction_object_id', 'Этаж', 'Тип этажа']).sum(numeric_only=True)[
                param_n].reset_index()
            all_morp = volumes_df['Морфотип секции'].unique()
            for morph in all_morp:
                one_morp_df = volumes_df[volumes_df['Морфотип секции'] == morph]
                sns.swarmplot(data=one_morp_df, y=param_n, x='Тип этажа', palette='Set1', legend=False,size=4)
                plt.title(f'{morph} {sk}')
                morph_name = morph
                if(('<' in morph_name)or('>' in morph_name)):
                    morph_name = str.replace(morph_name,'<','less')
                    morph_name = str.replace(morph_name,'>', 'more')
                plt.savefig(fr'{dir}\{pref_name}\{morph_name}-{sk}-swarmplot.png')
                plt.clf()


    def get_df_array_by_floor_sum(self,sk_df,df_full,sum_param):
        """
        Метод для получения словаря датафреймов по типам СК. В датафрейме каждая строка - этаж секции объекта и сумма параметра
        Parameters
        ----------
        df_full : (str)
            Полный датафрейм
        sk_df : pd.Dataframe
            Датафрейм со списком СК и списком имен параметров, по которым будет сбор
        sum_param : str
            Параметр по которому будет суммирование

        Returns
        -------
        {} pd.Dataframe
            Словарь датафреймов где ключ - имя ск, а значение - датафрейм с элементами
        """
        df_arr = {}
        sk_array = (tuple(sk_df['Имя СК'].array))
        for sk in sk_array:
            one_sk_df = df_full[df_full['Имя СК'] == sk]
            volumes_df = one_sk_df.groupby(['Морфотип секции','Секция','construction_object_id', 'Этаж', 'Тип этажа']).sum(numeric_only=True)[sum_param].reset_index()
            df_arr[sk] = volumes_df
        return df_arr

    def get_df_arr_sk_dev(self,dfFull,sk_df,co_df_info):
        """
        Метод получения словаря датафреймов по СК, где будут значения по этажам, эталон и отклонение
        Parameters
        ----------
        dfFull : pd.Dataframe
            Полный датафрейм
        sk_arr : []
            Массив имен СК
        param_name : (str)
            Имя параметра для суммирования
        co_df_info : pd.Dataframe
            Датафрейм с информацией о объектах и стадиях

        Returns
        -------
        {'str':pd.Dataframe}
            Ключ - имя СК, значение - Датафрейм
        """
        df_arr = {}
        sk_array = (tuple(sk_df['Имя СК'].array))
        for sk in sk_array:
            param_n = sk_df[sk_df['Имя СК'] == sk]['Имя параметра'].iloc[0]
            floors_sum_values = self.get_df_array_by_floor_sum(df_full=dfFull,sk_df=sk_df,sum_param=param_n)[sk]
            floors_sect_means = floors_sum_values.groupby(['Морфотип секции', 'Тип этажа']).mean(
                [param_n]).reset_index()
            divs_df = pd.merge(left=floors_sum_values, right=floors_sect_means, how='left',
                               on=['Морфотип секции', 'Тип этажа'])
            divs_df = divs_df.rename(columns={f'{param_n}_x': f'{param_n}', f'{param_n}_y': 'Эталон'})
            divs_df['Дельта, %'] = abs((divs_df[param_n] - divs_df['Эталон']) / divs_df['Эталон'] * 100)
            divs_df = pd.merge(left=divs_df,right=co_df_info,how='left',on='construction_object_id')
            divs_df = divs_df[['Морфотип секции','construction_object_id','name','Секция','Этаж','Тип этажа'
                ,param_n,'Эталон','Дельта, %']]

            df_arr[sk] = divs_df
        return  df_arr


    def get_standarts(self,dfFull,sk_df):
        """
        Метод получения словаря с датафреймами по эталонам
        Parameters
        ----------
        dfFull : (pd.Dataframe)
            Полный дф
        sk_df : pd.Dataframe
            Датафрейм со списком СК и списком имен параметров, по которым будет сбор
        param_name : (str)
            Имя параметра для сложения

        Returns
        -------
        {'str':pd.Dataframe}
            Ключ - имя ск, значение - датафрейм с эталонами
        """
        df_arr = {}
        sk_array = (tuple(sk_df['Имя СК'].array))
        for sk in sk_array:
            param_n = sk_df[sk_df['Имя СК'] == sk]['Имя параметра']
            floors_sum_values = self.get_df_array_by_floor_sum(df_full=dfFull,sk_df=sk_df,sum_param=param_n)[sk]
            floors_sect_means = floors_sum_values.groupby(['Морфотип секции', 'Тип этажа']).mean(
                [param_n]).reset_index()
            df_arr[sk] = floors_sect_means
        return df_arr

    def save_standarts(self,standarts_df_dict,sk_df,dir,pref):
        """
        Метод сохранения эксель таблиц по эталонам и отклонениям
        Parameters
        ----------
        standarts_df_dict : ({'str':pd.Dataframe})
            Словарь с ключом (СК) и значением (дф)
        sk_arr : []
            Список СК
        dir : (str)
            Директория
        pref : (str)
            Префикс для названия

        Returns
        -------
        None
            В директории создается на каждый датафрейм файл с именем pref_НазваниеСК.xlsx
        """
        sk_array = (tuple(sk_df['Имя СК'].array))
        for sk in sk_array:
            standarts_dict = standarts_df_dict[sk]
            standarts_dict.to_excel(dir+rf'\{pref}_{sk}.xlsx',index=False)

