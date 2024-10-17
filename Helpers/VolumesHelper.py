import pandas as pd
from Helpers.ParamsAndFuns import ParamsAndFuns as p
import seaborn as sns
import matplotlib.pyplot as plt


class VolumesHelper:
    def __init__(self,fullPath):
        """
        Конструктор класса, где нужно указать путь к csv файлу,для загрузки DF по помещениям

        Parameters
        -------
        fullPath: str
            Путь к файлу формата r'D:\Khabarov\RVT\Premises\TEP'\test.csv, разделитель ';'
        """
        # Конвертация данных
        fullDf = pd.read_csv(fullPath, sep=';')
        self.fullDf = fullDf.apply(p.convertToDouble)

    def save_boxplotes_for_morph_and_floor_types(self,sk_arr,df_full,pref_name,dir):
        for sk in sk_arr:
            one_sk_df = df_full[df_full['Имя СК'] == sk]
            volumes_df = one_sk_df.groupby(['Морфотип секции', 'name', 'Этаж', 'Тип этажа']).sum()[
                'Объем, м3'].reset_index()
            all_morp = volumes_df['Морфотип секции'].unique()
            for morph in all_morp:
                one_morp_df = volumes_df[volumes_df['Морфотип секции'] == morph]
                sns.boxplot(data=one_morp_df, y='Объем, м3', x='Тип этажа', palette='Set1', legend=False)
                plt.title(f'{morph} {sk}')
                plt.savefig(fr'{dir}\{pref_name}\{morph}-{sk}-boxplot.png')
                plt.clf()
                sns.swarmplot(data=one_morp_df, y='Объем, м3', x='Тип этажа', palette='Set1', legend=False,size=4)
                plt.title(f'{morph} {sk}')
                plt.savefig(fr'{dir}\{pref_name}\{morph}-{sk}-swarmplot.png')
                plt.clf()


    def get_df_array_by_floor_sum(self,df_full,sk_arr,sum_param):
        df_arr = {}
        for sk in sk_arr:
            one_sk_df = df_full[df_full['Имя СК'] == sk]
            volumes_df = one_sk_df.groupby(['Морфотип секции', 'name', 'Этаж', 'Тип этажа']).sum()[sum_param].reset_index()
            df_arr[sk] = volumes_df
        return df_arr

    def get_df_arr_sk_dev(self,dfFull,sk_arr,param_name):
        df_arr = {}
        for sk in sk_arr:
            floors_sum_values = self.get_df_array_by_floor_sum(dfFull, sk_arr, param_name)[sk]
            floors_sect_means = floors_sum_values.groupby(['Морфотип секции', 'Тип этажа']).mean(
                [param_name]).reset_index()
            divs_df = pd.merge(left=floors_sum_values, right=floors_sect_means, how='left',
                               on=['Морфотип секции', 'Тип этажа'])
            divs_df = divs_df.rename(columns={f'{param_name}_x': f'{param_name}', f'{param_name}_y': 'Эталон'})
            divs_df['Дельта, %'] = abs((divs_df[param_name] - divs_df['Эталон']) / divs_df['Эталон'] * 100)
            df_arr[sk] = divs_df
        return  df_arr

