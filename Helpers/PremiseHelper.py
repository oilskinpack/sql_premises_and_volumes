import numpy as np
import pandas as pd
from Helpers.ParamsAndFuns import ParamsAndFuns as p


class PremiseHelper:
    def __init__(self,df):
        """
        Конструктор класса, где нужно указать путь к csv файлу,для загрузки DF по помещениям

        Parameters
        -------
        fullPath: str
            Путь к файлу формата r'D:\Khabarov\RVT\Premises\TEP'\test.csv, разделитель ';'
        """
        self.fullDf = self.transposePremiseDf(df).apply(p.convert_to_double)

    def transposePremiseDf(self,dfPremiseParamValues):
        """
        Метод с помощью pd.pivot агрегирует данные по расчетам параметров помещений в удобную таблицу

        Parameters:
        ------------
        dfPremiseParamValues: pd.DataFrame
            Датафрейм со списком рассчитанных параметров по частям помещений, выгруженный из БД

        Returns: pd.DataFrame
            Датафрейм где строка - часть помещения (обобщенная модель), колонка - параметры

        """
        dfPremiseParamValues = dfPremiseParamValues[['model_version_element_id', 'title', 'value']]
        dfPremiseParamValues = pd.pivot(data=dfPremiseParamValues, index='model_version_element_id',
                                        columns='title', values='value')
        return dfPremiseParamValues

    def getDfOfSellPremisesByDest(self, destination):
        """
        Метод возвращает датафрейм с данными по частям помещений выбранного назначения (только продаваемые)

        Parameters:
        ------------
        destination: str
                Принимает следующие значения ['Жилье','Ритейл','Кладовки','Паркинг']

        Returns: pd.DataFrame
            Датафрейм выбранного назначения,где строка - часть помещения (обобщенная модель), колонка - параметры

        """
        fullDf = self.fullDf

        #Собираем датафрейм
        if destination != 'Паркинг':
            destDf = fullDf[(fullDf[p.bru_destination_pn] == destination) & (fullDf[p.bru_type_pn] != 'МОП')]
        else:
            destDf = fullDf[(fullDf[p.type_pn] == 'Машино-место')]
        # Превращаем в инты основные параметры
        destDf = destDf.apply(p.convert_to_double)
        if destination != 'Паркинг':
            destDf = destDf.sort_values([p.bru_floor_int_pn, p.bru_section_int_pn, p.adsk_index_int_pn])
        else:
            destDf = destDf.sort_values([p.bru_floor_int_pn, p.bru_floor_int_pn, p.adsk_index_int_pn])
        return destDf

    def getDfOfSellPremisesByDestGrouped(self, destination):
        """
        Метод возвращает датафрейм с данными по частям помещений выбранного назначения (только продаваемые),
        сгруппированные по Номеру квартиры

        Parameters:
        -----------
        destination: str
             Принимает следующие значения ['Жилье','Ритейл','Кладовки','Паркинг']

        Returns: pd.DataFrame
            Датафрейм выбранного назначения,где строка - помещение, колонка - параметры
        """
        destDf = self.getDfOfSellPremisesByDest(destination)
        flatDfGrouped = destDf.groupby(p.adsk_premise_number).min(numeric_only=True)
        return flatDfGrouped

    def getSellCount(self,destination):
        """
        Получение количества продаваемых лотов для выбранного назначения

        Parameters:
        -----------
        destination: str
             Принимает следующие значения ['Жилье','Ритейл','Кладовки','Паркинг']
        Returns: int
            Количество лотов
        """
        count = len(self.getDfOfSellPremisesByDestGrouped(destination))
        return count

    def getSellArea(self,destination,byPart = True ):
        """
        Получение продаваемой площади по выбранному назначению помещений

        Parameters:
        -----------
        destination: str
             Принимает следующие значения ['Жилье','Ритейл','Кладовки','Паркинг']
        byPart:
            True - расчет будет вестись по площадям частей помещений
            False - расчет будет вестись по площадям помещений

        Returns: float
            Значение площади
        """
        if byPart:
            sum = self.getDfOfSellPremisesByDest(destination)[p.bru_premise_part_area_pn].sum()
        else:
            sum = self.getDfOfSellPremisesByDestGrouped(destination)[p.bru_premise_full_area_pn].sum()
        return sum

    def getFlatTypeMatrix(self,byExpertise = True):
        """
        Метод считает матрицу типов квартир в объекте

        Parameters:
        -----------
        byExpertise: bool
            True - по экспертизе
            False - по продажам

        Returns: pd.DataFrame
            Датафрейм с матрицей по типам квартир и количеству

        """
        df = self.getDfOfSellPremisesByDest('Жилье')
        if byExpertise:
            matrix = df.groupby([p.adsk_type_pn, p.adsk_premise_number]).size().groupby(p.adsk_type_pn).size()
        else:
            matrix = df.groupby([p.bru_type_pn, p.adsk_premise_number]).size().groupby(p.bru_type_pn).size()
        return matrix

    def getCommonPremisesDf(self):
        """
        Метод получение датафрейма по помещениям МОП объекта, по виду как для ДОМ.рф

        Returns: pd.DataFrame
            Датафрейм по формату ДОМ.рф для МОП

        """
        fullDf = self.fullDf
        mopDf = fullDf[fullDf[p.type_pn] == 'МОП'].sort_values(
            [p.bru_section_int_pn, p.bru_floor_int_pn, p.adsk_premise_number])[
            [p.name_pn, p.bru_premise_part_area_pn, p.bru_section_int_pn, p.bru_floor_int_pn]]

        mopDf['Назначение помещения'] = 'Общественное'
        mopDf['Описание местоположения помещения'] = 'Секция ' + mopDf[p.bru_section_int_pn]
        mopDf = mopDf.rename(columns={p.name_pn: 'Вид помещения'})
        mopDf = mopDf.rename(columns={p.bru_premise_part_area_pn: 'Площадь,м²'})
        mopDf = mopDf.reset_index()[
            ['Вид помещения', 'Описание местоположения помещения', 'Назначение помещения', 'Площадь,м²']]
        mopDf['Вид помещения'] = mopDf['Вид помещения'].str.replace(pat='Автостоянка', repl='Автостоянка (с учетом мм)')
        return mopDf

    def getTechPremisesDf(self):
        """
        Метод получения датафрейма по техническим помещениям с маппингом вида помещения и оборудования

        Returns: pd.DataFrame
            Датафрейм технических помещений для дом.рф

        """
        common_df = self.getDfOfSellPremisesByDest('МОП')
        lk_df = common_df[common_df[p.name_pn].str.contains('лестничная', False) == True]
        max_lk = lk_df.groupby(p.bru_section_int_pn).max(numeric_only = True).reset_index()
        max_lk ['Имя'] = 'Вертикальный транспорт'
        max_lk = max_lk[[p.name_pn, p.bru_premise_part_area_pn, p.bru_section_int_pn, p.bru_floor_int_pn]].reset_index()
        max_lk = max_lk.apply(p.convert_to_double)

        fullDf = self.fullDf
        techDf = fullDf[fullDf[p.bru_destination_pn] == 'Техническое'].sort_values(
            [p.bru_section_int_pn, p.bru_floor_int_pn, p.adsk_premise_number])[
            [p.name_pn, p.bru_premise_part_area_pn, p.bru_section_int_pn, p.bru_floor_int_pn]].reset_index()
        techDf = techDf.apply(p.convert_to_double)

        techDf = pd.concat([techDf,max_lk],axis=0)
        techDf = techDf.sort_values([p.bru_section_int_pn, p.bru_floor_int_pn], ascending=[True, True])

        techDf[p.bru_section_int_pn] = techDf[p.bru_section_int_pn].astype(str).str.split('.').str[0]
        techDf['Описание местоположения помещения'] = ''
        techDf['Часть'] = ''
        techDf = techDf.rename(columns={p.name_pn: 'Назначение'})
        techDf['Вид оборудования'] = techDf['Назначение'].apply(p.define_tech_equipment)
        techDf['Вид оборудования'] = np.where(((techDf['Вид оборудования'] == 'Лифт') & (techDf[p.bru_floor_int_pn] < 15)), 'Лифт, 2 шт.', techDf['Вид оборудования'])
        techDf['Вид оборудования'] = np.where(((techDf['Вид оборудования'] == 'Лифт') & (techDf[p.bru_floor_int_pn] >= 15)), 'Лифт, 4 шт.', techDf['Вид оборудования'])


        techDf['Часть'] = np.where(techDf['Вид оборудования'].str.contains('электро') == True,'Подземная часть, ',techDf['Часть'])
        techDf['Часть'] = np.where(techDf['Вид оборудования'].str.contains('Лифт') == True,'',techDf['Часть'])
        techDf['Часть'] = np.where(techDf['Вид оборудования'] == '','Подземная и надземная части, ',techDf['Часть'])
        techDf['Описание местоположения помещения'] = techDf['Часть']+ 'Секция ' + techDf[p.bru_section_int_pn].astype(str)
        techDf = techDf.reset_index()

        techDf = techDf[['Описание местоположения помещения', 'Вид оборудования', 'Назначение']]
        return techDf

    def getConstrObjParameters(self):
        """
        Метод получения датафрейма с основными площадями и количествами помещений

        Returns: pd.DataFrame
            Датафрейм с данными по объекту

        """
        #Получаем датафреймы по категориям
        flatsDf = self.getDfOfSellPremisesByDest('Жилье')
        retailDf = self.getDfOfSellPremisesByDest('Ритейл')
        carsDf = self.getDfOfSellPremisesByDest('Паркинг')
        pantriesDf = self.getDfOfSellPremisesByDest('Кладовки')

        #Считаем площади
        fullFlatAreaParts = self.getSellArea('Жилье')
        retailAreaParts = self.getSellArea('Ритейл')
        carsAreaParts = self.getSellArea('Паркинг')
        pantriesAreaParts = self.getSellArea('Кладовки')

        #Количества
        flatCount = self.getSellCount('Жилье')
        retailCount = self.getSellCount('Ритейл')
        carsCount = self.getSellCount('Паркинг')
        pantriesCount = self.getSellCount('Кладовки')

        summerNoTerraseArea = \
        flatsDf[(flatsDf[p.name_pn].str.contains('Терраса') == False) & (flatsDf[p.name_pn].isin(p.summer_area_names))][
            p.bru_premise_part_area_pn].sum()
        terraseArea = flatsDf[(flatsDf[p.name_pn].str.contains('Терраса') == True) & (flatsDf[p.name_pn].isin(p.summer_area_names))][
            p.bru_premise_part_area_pn].sum()
        retailBelowZeroArea = retailDf[retailDf[p.adsk_premise_number].str.contains('.-') == True][
            p.bru_premise_part_area_pn].sum()
        carsBelowZeroArea = carsDf[carsDf[p.bru_floor_int_pn] <= -1][p.bru_premise_part_area_pn].sum()
        pantriesBelowZeroArea = pantriesDf[pantriesDf[p.bru_floor_int_pn] <= -1][p.bru_premise_part_area_pn].sum()

        rowNames = ['Жилье с ЛП', '   в т.ч. летние помещения (без террас)', '   в т.ч. террасы',
                    'Коммерческие помещения', 'Паркинг', 'Кладовые']

        constrObjDf = pd.DataFrame(index=rowNames)
        constrObjDf['Прод.площадь'] = [fullFlatAreaParts, summerNoTerraseArea, terraseArea, retailAreaParts,
                                       carsAreaParts, pantriesAreaParts]
        constrObjDf['в т.ч. ниже ур.з'] = [0, 0, 0, retailBelowZeroArea, carsBelowZeroArea, pantriesBelowZeroArea]
        constrObjDf['Кол.'] = [flatCount, 0, 0, retailCount, carsCount, pantriesCount]
        return constrObjDf

    def getCalcCmrDf(self):
        """
        Метод получения датафрейма с данными для калькулятора бюджета по объекту

        Returns: pd.DataFrame
            Датафрейм с основными высчитываемыми данными по помещениям и площадям ОС

        """
        fullDf = self.fullDf.apply(p.convert_to_double)
        # Получаем датафреймы по категориям
        flatsDf = self.getDfOfSellPremisesByDest('Жилье')

        # Считаем площади
        fullFlatAreaParts = self.getSellArea('Жилье')
        retailAreaParts = self.getSellArea('Ритейл')
        carsAreaParts = self.getSellArea('Паркинг')
        pantriesAreaParts = self.getSellArea('Кладовки')

        #Количества
        carsCount = int(self.getSellCount('Паркинг'))

        # fullPremiseDf = fullDf[
        #     fullDf[p.BruDestinationPn].isin(['Жилье', 'МОП', 'Ритейл', 'Паркинг', 'Кладовки', 'Техническое'])]
        # gnsDf = fullDf[fullDf[p.BruDestinationPn] == 'ГНС']

        # fullObjectArea = fullPremiseDf[p.BruPremisePartAreaPN].sum()  # Общая площадь объекта
        saleObjectArea = fullFlatAreaParts + retailAreaParts + pantriesAreaParts + carsAreaParts  # Продаваемая площадь объекта
        # fullBuildingArea = gnsDf[gnsDf[p.SectionStrPN] != 'Паркинг'][
        #     p.BruPremisePartAreaPN].sum()  # Общая площадь здания
        # fullTypeFloorArea = fullPremiseDf[fullPremiseDf['BRU_Этаж_Число'] == 3][
        #     p.BruPremisePartAreaPN].sum()  # Общая площадь типового этажа
        # saleTypeFloorArea = \
        # fullPremiseDf[(fullPremiseDf[p.BruFloorIntPN] == 3) & (fullPremiseDf[p.BruDestinationPn] == 'Жилье')][
        #     p.BruPremisePartAreaPN].sum()  # Прод площадь типового этажа

        # gnsArea = gnsDf[gnsDf[p.SectionStrPN] != 'Паркинг'][p.BruPremisePartAreaPN].sum()  # Площадь в ГНС

        loggiaArea = flatsDf[flatsDf[p.name_pn].str.contains('Лоджия') == True][p.bru_premise_part_area_pn].sum()
        terraseOnRoofArea = flatsDf[flatsDf[p.name_pn] == 'Терраса'][p.bru_premise_part_area_pn].sum()
        terraseOnGroundArea = flatsDf[flatsDf[p.name_pn] == 'Терраса на земле'][p.bru_premise_part_area_pn].sum()
        balconyArea = flatsDf[flatsDf[p.name_pn].str.contains('Балкон') == True][p.bru_premise_part_area_pn].sum()
        mopArea = round(fullDf[fullDf[p.type_pn] == 'МОП'].apply(p.convert_to_double)[p.bru_premise_part_area_pn].sum(), 2)
        techArea = round(fullDf[fullDf[p.type_pn] == 'Технические помещения'].apply(p.convert_to_double)[p.bru_premise_part_area_pn].sum(), 2)
        typicalFloorArea = fullDf[(fullDf[p.bru_floor_int_pn] == 3) & (fullDf[p.bru_category_pn].notnull())][
            p.bru_premise_part_area_pn].sum()
        typicalSaleArea = fullDf[(fullDf[p.bru_floor_int_pn] == 3) & (fullDf[p.bru_category_pn].notnull()) & (
                fullDf[p.type_pn] != 'МОП')][p.bru_premise_part_area_pn].sum()
        fullParkingArea = \
        fullDf[(fullDf[p.bru_category_pn] == 'Паркинг') & (fullDf[p.name_pn].str.contains('Автостоянк'))][
            p.bru_premise_part_area_pn].sum()
        if(carsCount == 0):
            meanParkingArea = 0
        else:
            meanParkingArea = carsAreaParts / carsCount



        calcCMR = pd.DataFrame(columns=['Наименование', 'Площадь,м2'])
        calcCMR.loc[len(calcCMR.index)] = ['Продаваемая площадь здания', saleObjectArea]
        calcCMR.loc[len(calcCMR.index)] = ['Жилье', fullFlatAreaParts]
        calcCMR.loc[len(calcCMR.index)] = ['Коммерческие помещения', retailAreaParts]
        calcCMR.loc[len(calcCMR.index)] = ['Кладовые', pantriesAreaParts]
        calcCMR.loc[len(calcCMR.index)] = ['Террасы на кровле', terraseOnRoofArea]
        calcCMR.loc[len(calcCMR.index)] = ['Террасы на земле', terraseOnGroundArea]
        calcCMR.loc[len(calcCMR.index)] = ['Балконы', balconyArea]
        calcCMR.loc[len(calcCMR.index)] = ['Лоджии', loggiaArea]
        calcCMR.loc[len(calcCMR.index)] = ['МОП', mopArea]
        calcCMR.loc[len(calcCMR.index)] = ['Технические помещения', techArea]
        calcCMR.loc[len(calcCMR.index)] = ['Общая площадь типового этажа', typicalFloorArea]
        calcCMR.loc[len(calcCMR.index)] = ['Продаваемая площадь  типового этажа', typicalSaleArea]
        calcCMR.loc[len(calcCMR.index)] = ['Общая площадь паркинга', fullParkingArea]
        calcCMR.loc[len(calcCMR.index)] = ['Продаваемая площадь паркинга', carsAreaParts]
        calcCMR.loc[len(calcCMR.index)] = ['Количество машиномест', int(carsCount)]
        calcCMR.loc[len(calcCMR.index)] = ['Площадь одного машиноместа', round(meanParkingArea, 2)]
        return calcCMR

    def getWindowsDf(self):
        '''
        Метод получения датафрейма с информацией по окнам

        Returns
        -------
        pd.Dataframe
            Датафрейм с окнами
        '''
        fullDf = self.fullDf
        windowsDf = fullDf[
            (fullDf[p.bru_destination_pn] == 'Окно') | (fullDf[p.bru_destination_pn] == 'Витраж')]

        windowsFlats = windowsDf[(windowsDf['Номер части помещения To'].str.contains('Ж') == True) | (
                    windowsDf['Номер части помещения From'].str.contains('Ж') == True)]
        windowsFlats = windowsFlats[windowsFlats[p.bru_destination_pn] != 'Витраж']
        windowsFlats = windowsFlats[p.window_colums]

        return windowsFlats

    def getWindowFlatsOrienDf(self):
        '''
        Получение датафрейма с ориентацией окон жилья

        Returns
        -------
        pd.Dataframe
            Датафрейм с инфой по окнам жилья
        '''
        windowsFlats = self.getWindowsDf()
        windowsFlats['Номер помещения'] = np.where(windowsFlats['Номер помещения To'] is np.nan,
                                                   windowsFlats['Номер помещения To'],
                                                   windowsFlats['Номер помещения From'])
        windowsFlats = windowsFlats.groupby('Номер помещения', as_index=False)['Сторона света'].agg(','.join)
        windowsFlats['Уникальные стороны'] = windowsFlats.reset_index()['Сторона света'].apply(p.get_unique)
        windowsFlats['Кол-во сторон'] = windowsFlats['Уникальные стороны'].apply(p.count_items)
        return windowsFlats

    def getGnsDf(self):
        '''
        Получение датафрейма по ГНС

        Returns
        -------
        pd.Dataframe
            Датафрейм по ГНС
        '''
        fullDf = self.fullDf
        gnsDf = fullDf[fullDf[p.bru_destination_pn] == 'ГНС']
        gnsGrouped = gnsDf[[p.section_str_pn, p.bru_floor_int_pn, p.bru_premise_part_area_pn]].groupby(
            [p.section_str_pn, p.bru_floor_int_pn]).sum()
        return gnsGrouped

    def get_flats_with_summer_premises(self):
        '''
        Получение Датафрейма с информацией по летним помещениям квартир (попадают только квартиры с ЛП)

        Returns
        -------
        pd.Dataframe
            Датафрейм с информацией об ЛП
        '''
        flatsDf = self.getDfOfSellPremisesByDest('Жилье')
        flatsDf  = flatsDf [flatsDf [p.bru_premise_summer_area_pn] > 0]
        flatsWithSummerPremises = flatsDf[
            [p.adsk_premise_number,p.has_terrase_on_roof, p.has_terrase_on_floor, p.has_balcony, p.has_cold_loggia, p.has_warm_loggia,
             p.bru_premise_summer_area_pn]]
        return flatsWithSummerPremises

    def get_summer_areas(self):
        '''
        Получение суммы площадей ЛП по разным ЛП

        Returns
        -------
        np.Series
            Данные по площадям
        '''
        flats = self.getDfOfSellPremisesByDest('Жилье')
        flats = flats[flats[p.name_pn].isin(['Балкон', 'Лоджия', 'Лоджия (холодная)', 'Терраса', 'Терраса на земле','Лоджия (техническая)'])]
        res = flats.groupby(p.name_pn).sum()[p.bru_premise_part_area_pn]
        return  res

    def duplex_flats(self):
        '''
        Выводит список квартир в два уровня
        Returns
        -------
        pd.Dataframe
            Датафрейм со список квартир
        '''
        flatPrem = self.getDfOfSellPremisesByDest('Жилье')
        flats_levels_count_df = (flatPrem.groupby([p.adsk_premise_number, p.bru_floor_int_pn]).size().reset_index()
                                 .groupby([p.adsk_premise_number]).size())
        res = flats_levels_count_df[flats_levels_count_df > 1]
        return res

    def show_premise_info(self,number,params_arr):
        '''
        Показывает интересующие поля у нужного помещения (выбор по номеру помещения)

        Parameters
        ----------
        number:(str)
            Номер помещения (ищется по contains)
        params_arr:[]
            Массив параметров для вывода

        Returns:pd.Dataframe
            Датафрейм с информацией
        -------

        '''
        premises = self.fullDf
        res = premises[premises['Номер помещения'].str.contains(number) == True][params_arr]
        return res

    def areas_info(self,dest):
        flats = self.getDfOfSellPremisesByDest(dest)
        noCoefAreaPart = flats[p.bru_premise_part_area_pn].sum(numeric_only=True)
        noCoefAreaFull = flats.groupby(p.adsk_premise_number).min(numeric_only=True).sum()[p.bru_premise_full_area_pn]
        noCoefAreaCommon = flats.groupby(p.adsk_premise_number).min(numeric_only=True).sum()[
            p.bru_premise_common_area_pn]
        noSummerAreaFull = flats.groupby(p.adsk_premise_number).min(numeric_only=True).sum()[
            p.bru_premise_non_summer_area_pn]
        summerAreaFull = flats.groupby(p.adsk_premise_number).min(numeric_only=True).sum()[p.bru_premise_summer_area_pn]
        livingAreaFull = flats.groupby(p.adsk_premise_number).min(numeric_only=True).sum()[p.bru_premise_living_area_pn]

        df = pd.DataFrame(
            index=['Площадь без коэф (части помещений)', 'Площадь без коэф', 'Площадь с коэф', 'Площадь без ЛП',
                   'Площадь ЛП', 'Жилая площадь']
            , data=[noCoefAreaPart, noCoefAreaFull, noCoefAreaCommon, noSummerAreaFull, summerAreaFull, livingAreaFull],
            columns=['Площадь м2'])
        return df

    def premises_with_dif_areas(self,dest):
        flats = self.getDfOfSellPremisesByDest(dest)
        dfFullArea = flats.groupby(p.adsk_premise_number).min(numeric_only=True)[
            p.bru_premise_full_area_pn].reset_index()
        res = dfFullArea
        dfPartArea = flats.groupby(p.adsk_premise_number).sum(numeric_only=True)[
            p.bru_premise_part_area_pn].reset_index()
        res = dfPartArea
        full = pd.merge(left=dfFullArea, right=dfPartArea, how='left', on='Номер квартиры')
        res = full[round(full['Площадь квартиры без коэффициентов'], 2) != round(full['Площадь помещения'], 2)]
        return res
    
    def sfa_and_gfa_areas(self):
        all_parts = self.fullDf.apply(p.convert_to_double)
        pantries = self.getDfOfSellPremisesByDest("Кладовки").apply(p.convert_to_double)
        #region Расчет SFA
        sfa_parking = (self.getSellArea('Паркинг', True)
                       + (pantries[pantries[p.section_str_pn].str.contains("Паркинг",False) == True][p.bru_premise_part_area_pn].sum()))
        sfa_house = (pantries[pantries[p.section_str_pn].str.contains("Паркинг",False) == False][p.bru_premise_part_area_pn].sum()
                     + self.getSellArea("Ритейл",True)
                     + self.getSellArea("Жилье",True))
        #endregion
        #region Расчет GFA
        gfa_parking = (all_parts[(all_parts[p.section_str_pn].str.contains('паркинг',False) == True) & (all_parts[p.type_pn] == 'МОП')][p.bru_premise_part_area_pn].sum()
                       + all_parts[(all_parts[p.section_str_pn].str.contains('паркинг',False) == True) & (all_parts[p.type_pn] == 'Технические помещения')][p.bru_premise_part_area_pn].sum())

        common_house = \
        all_parts[(all_parts[p.section_str_pn].str.contains('паркинг', False) == False) & (all_parts[p.type_pn] == 'МОП')][
            p.bru_premise_part_area_pn].sum()
        tech_house = all_parts[(all_parts[p.section_str_pn].str.contains('паркинг', False) == False) & (
                    all_parts[p.type_pn] == 'Технические помещения')][p.bru_premise_part_area_pn].sum()
        pantries_house_area = pantries[pantries[p.section_str_pn].str.contains("Паркинг", False) == False][
            p.bru_premise_part_area_pn].sum()
        retail_area = self.getSellArea("Ритейл", True)
        flats_area = self.getSellArea("Жилье", True)

        gfa_house = common_house + tech_house + flats_area + retail_area + pantries_house_area


        #endregion
        gfa_sfa_df = pd.DataFrame(columns=['Площадь, м2']
                                  , data=[sfa_house, gfa_house, sfa_parking, gfa_parking]
                                  , index=['SFA дом', 'GFA дом', 'SFA паркинг', 'GFA паркинг'])
        return gfa_sfa_df

    def technical_economic_values(self):
        all_parts = self.fullDf.apply(p.convert_to_double)
        gns = self.getGnsDf()
        flats = self.getDfOfSellPremisesByDest('Жилье').apply(p.convert_to_double)
        retails = self.getDfOfSellPremisesByDest('Ритейл').apply(p.convert_to_double)
        pantries = self.getDfOfSellPremisesByDest('Кладовки').apply(p.convert_to_double)

        development_area = (all_parts[(all_parts[p.bru_destination_pn] == 'ГНС') & (all_parts[p.bru_floor_int_pn].astype(float) == 1)]
                .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum()).rename('Площадь застройки по СП54.13330.2022')
        # region Этажи
        # Этажность
        below_zero_floors = all_parts.groupby(p.section_str_pn)[p.bru_floor_int_pn].max().rename('Этажность')
        # Кол-во этажей
        floors_count = (all_parts.groupby([p.section_str_pn, p.bru_floor_int_pn]).size().reset_index().groupby(
            [p.section_str_pn]).size()
                        .rename('Кол-во этажей,в том числе:'))
        # Подземных этажей
        under_zero_floors = (
            all_parts[all_parts[p.bru_floor_int_pn] < 0].groupby([p.section_str_pn, p.bru_floor_int_pn]).size()
            .reset_index().groupby([p.section_str_pn]).size().rename('        - подземных этажей'))

        # endregion
        # region Количество квартир
        # Количество всех квартир
        flats_count = (flats.groupby([p.section_str_pn, p.adsk_premise_number]).size().reset_index().groupby(
            [p.section_str_pn]).size()
                       .rename('Кол-во квартир'))

        # region 1-комнатные
        # Кол-во 1-комн квартир
        all_one_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['С', '1С', '1К', '1Д', 'СП', 'СД']))
                                          & (flats[p.rooms_count].isin([1, 0]))].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                    .reset_index()).groupby([p.section_str_pn]).size().rename(
            'Квартира 1-комнатная,в том числе:')

        # Кол-во 1С квартир
        one_room_flats_count = (flats[flats[p.adsk_type_pn].isin(['1С', '1К', '1Д'])].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                .reset_index().groupby([p.section_str_pn]).size()
                                .rename('        - тип 1С'))
        # Кол-во С квартир
        zero_room_flats_count = (
            flats[flats[p.adsk_type_pn].isin(['С'])].groupby([p.section_str_pn, p.adsk_premise_number]).size()
            .reset_index().groupby([p.section_str_pn]).size()
            .rename('        - тип С'))
        # Кол-во СП квартир
        one_free_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СП']))
                                           & (flats[p.rooms_count] == 1)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                     .reset_index().groupby([p.section_str_pn]).size()
                                     .rename('        - тип 1СП'))
        # Кол-во СД квартир
        one_duplex_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СД']))
                                             & (flats[p.rooms_count] == 1)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                       .reset_index().groupby([p.section_str_pn]).size()
                                       .rename('        - тип 1СД'))
        # endregion
        # region 2-комнатные
        # Кол-во 2-комн квартир
        all_two_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['2С', '2К', '2Д', 'СП', 'СД']))
                                          & (flats[p.rooms_count].isin([2]))].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                    .reset_index()).groupby([p.section_str_pn]).size().rename(
            'Квартира 2-комнатная,в том числе:')

        # Кол-во 2С квартир
        two_room_flats_count = (flats[flats[p.adsk_type_pn].isin(['2С', '2К', '2Д'])].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                .reset_index().groupby([p.section_str_pn]).size()
                                .rename('        - тип 2С'))
        # Кол-во 2СП квартир
        two_free_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СП']))
                                           & (flats[p.rooms_count] == 2)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                     .reset_index().groupby([p.section_str_pn]).size()
                                     .rename('        - тип 2СП'))
        # Кол-во 2СД квартир
        two_duplex_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СД']))
                                             & (flats[p.rooms_count] == 2)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                       .reset_index().groupby([p.section_str_pn]).size()
                                       .rename('        - тип 2СД'))

        # endregion
        # region 3-комнатная
        # Кол-во 3-комн квартир
        all_three_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['3С', '3К', '3Д', 'СП', 'СД']))
                                            & (flats[p.rooms_count].isin([3]))].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                      .reset_index()).groupby([p.section_str_pn]).size().rename(
            'Квартира 3-комнатная,в том числе:')

        # Кол-во 3С квартир
        three_room_flats_count = (flats[flats[p.adsk_type_pn].isin(['3С', '3К', '3Д'])].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                  .reset_index().groupby([p.section_str_pn]).size()
                                  .rename('        - тип 3С'))
        # Кол-во 3СП квартир
        three_free_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СП']))
                                             & (flats[p.rooms_count] == 3)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                       .reset_index().groupby([p.section_str_pn]).size()
                                       .rename('        - тип 3СП'))
        # Кол-во 3СД квартир
        three_duplex_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СД']))
                                               & (flats[p.rooms_count] == 3)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                         .reset_index().groupby([p.section_str_pn]).size()
                                         .rename('        - тип 3СД'))

        # endregion
        # region 4-комнатная
        # Кол-во 4-комн квартир
        all_four_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['4С', '4К', '4Д', 'СП', 'СД']))
                                           & (flats[p.rooms_count].isin([4]))].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                     .reset_index()).groupby([p.section_str_pn]).size().rename(
            'Квартира 4-комнатная,в том числе:')

        # Кол-во 4С квартир
        four_room_flats_count = (flats[flats[p.adsk_type_pn].isin(['4С', '4К', '4Д'])].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                 .reset_index().groupby([p.section_str_pn]).size()
                                 .rename('        - тип 4С'))
        # Кол-во 4СП квартир
        four_free_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СП']))
                                            & (flats[p.rooms_count] == 4)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                      .reset_index().groupby([p.section_str_pn]).size()
                                      .rename('        - тип 4СП'))
        # Кол-во 4СД квартир
        four_duplex_room_flats_count = (flats[(flats[p.adsk_type_pn].isin(['СД']))
                                              & (flats[p.rooms_count] == 4)].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                                        .reset_index().groupby([p.section_str_pn]).size()
                                        .rename('        - тип 4СД'))

        # endregion
        # endregion
        # region Площади квартир
        # Площадь жилая
        living_flat_area_full = (flats[flats[p.adsk_premise_part_number].str.endswith('.1')]
                                 .groupby([p.section_str_pn, p.adsk_premise_number]).sum().reset_index().groupby(
            [p.section_str_pn])
                                 [p.bru_premise_living_area_pn].sum().rename('Жилая площадь квартир'))
        # Площадь без ЛП
        no_summer_flat_area_full = (flats[flats[p.adsk_premise_part_number].str.endswith('.1')]
                                    .groupby([p.section_str_pn, p.adsk_premise_number]).sum().reset_index().groupby(
            [p.section_str_pn])
                                    [p.bru_premise_non_summer_area_pn].sum().rename(
            'Общая площадь квартир без летних помещений'))
        # Площадь с коэф
        koef_flat_area_full = (flats[flats[p.adsk_premise_part_number].str.endswith('.1')]
                               .groupby([p.section_str_pn, p.adsk_premise_number]).sum().reset_index().groupby(
            [p.section_str_pn])
                               [p.bru_premise_common_area_pn].sum().rename(
            'Общая площадь квартир с учетом коэф. для ЛП'))
        # Площадь без коэф
        no_koef_flat_area_full = (
            flats.groupby([p.section_str_pn, p.adsk_premise_number]).sum().reset_index().groupby([p.section_str_pn])
            [p.bru_premise_part_area_pn].sum().rename('Общая площадь без коэф.'))

        # endregion
        # region Кол-во встроенных помещений

        # region Коммерция
        # Кол-во коммерческих помещений
        retail_count = (
            retails.groupby([p.section_str_pn, p.adsk_premise_number]).size().groupby(p.section_str_pn).size()
            .rename('Кол-во коммерческих помещений, в том числе:'))
        # Кол-во УК
        retail_MC = (retails[retails[p.type_pn] == 'Управляющая компания'].groupby(
            [p.section_str_pn, p.adsk_premise_number]).size()
                     .groupby(p.section_str_pn).size()
                     .rename('        - помещения УК'))
        # endregion
        # region Кладовки
        # Кол-во всех кладовых
        pantries_all = (pantries
                        .groupby([p.section_str_pn, p.adsk_premise_number]).size()
                        .groupby(p.section_str_pn).size()
                        .rename('Кол-во помещений кладовых, в том числе:'))

        # Кол-во индивидуальных кладовых
        pantries_indiv = (pantries[(pantries[p.section_str_pn].str.contains('паркинг', False) == False)]
                          .groupby([p.section_str_pn, p.adsk_premise_number]).size()
                          .groupby(p.section_str_pn).size()
                          .rename('        - индивидуальных кладовых'))

        # Кол-во кладовых паркинга
        pantries_parking = (pantries[(pantries[p.section_str_pn].str.contains('паркинг', False) == True)]
                            .groupby([p.section_str_pn, p.adsk_premise_number]).size()
                            .groupby(p.section_str_pn).size()
                            .rename('        - кладовых паркинга'))
        # endregion
        # region Велосипедные
        bicycles_count = (all_parts[(all_parts[p.type_pn] == 'МОП') & (
                    all_parts[p.name_pn].str.contains('Велосипедная', False) == True)]
                          .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].size().rename(
            'Количество мест хранения - велосипедные'))

        # endregion
        # endregion
        # region Вместимость автостоянки

        cars_count = all_parts[all_parts[p.type_pn] == 'Машино-место'].groupby(p.section_str_pn).size().rename(
            'Вместимость автостоянки')

        # endregion
        # region Площадь встроенных помещений

        # region Коммерция
        # Кол-во коммерческих помещений
        retail_area = (retails.groupby([p.section_str_pn, p.adsk_premise_number]).sum().groupby(p.section_str_pn)[
                           p.bru_premise_part_area_pn].sum()
                       .rename('Площадь коммерческих помещений, в том числе:'))
        # Кол-во УК
        retail_MC_area = (retails[retails[p.type_pn] == 'Управляющая компания'].groupby(
            [p.section_str_pn, p.adsk_premise_number]).sum()
                          .groupby(p.section_str_pn).sum()[p.bru_premise_part_area_pn]
                          .rename('        - помещения УК'))
        # endregion
        # region Кладовки
        # Кол-во всех кладовых
        pantries_all_area = (pantries
                             .groupby([p.section_str_pn, p.adsk_premise_number]).sum()
                             .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum()
                             .rename('Площадь помещений кладовых, в том числе:'))

        # Кол-во индивидуальных кладовых
        pantries_indiv_area = (pantries[(pantries[p.section_str_pn].str.contains('паркинг', False) == False)]
                               .groupby([p.section_str_pn, p.adsk_premise_number]).sum()
                               .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum()
                               .rename('        - индивидуальных кладовых'))

        # Кол-во кладовых паркинга
        pantries_parking_area = (pantries[(pantries[p.section_str_pn].str.contains('паркинг', False) == True)]
                                 .groupby([p.section_str_pn, p.adsk_premise_number]).sum()
                                 .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum()
                                 .rename('        - кладовых паркинга'))
        # endregion
        # region Велосипедные
        bicycles_area = (all_parts[(all_parts[p.type_pn] == 'МОП') & (
                    all_parts[p.name_pn].str.contains('Велосипедная', False) == True)]
                         .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum().rename(
            'Площадь мест хранения - велосипедные'))

        # endregion

        # endregion
        # region Площадь машино-мест

        cars_area = (
            all_parts[all_parts[p.type_pn] == 'Машино-место'].groupby(p.section_str_pn)[p.bru_premise_part_area_pn]
            .sum().rename('Площадь машино-мест'))

        # endregion
        # region Кол-во мест общего пользования

        # МОП секции
        common_sect_count = ((all_parts[(all_parts[p.type_pn] == 'МОП')
                                        & (all_parts[p.section_str_pn].str.contains('паркинг', False) == False)])
                             .groupby(p.section_str_pn).size().rename('МОП секции'))
        # МОП паркинга
        common_parking_count = ((all_parts[(all_parts[p.type_pn] == 'МОП')
                                           & (all_parts[p.section_str_pn].str.contains('паркинг', False) == True)])
                                .groupby(p.section_str_pn).size().rename('МОП паркинга, в том числе'))
        # Автостоянка
        park_names = ['Автостоянка'
            , 'Рампа'
            , 'Въездная-выездная полоса']
        commop_parking_park_count = (all_parts[all_parts[p.name_pn].isin(park_names)].groupby(p.section_str_pn).size()
                                     .rename('МОП автостоянки, включая рампу'))

        # endregion
        # region Площадь мест общего пользования

        # МОП секции
        common_sect_area = ((all_parts[(all_parts[p.type_pn] == 'МОП')
                                       & (all_parts[p.section_str_pn].str.contains('паркинг', False) == False)])
                            .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum().rename('Площадь МОП секции'))
        # МОП паркинга
        common_parking_area = ((all_parts[(all_parts[p.type_pn] == 'МОП')
                                          & (all_parts[p.section_str_pn].str.contains('паркинг', False) == True)])
                               .groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum().rename('Площадь'))
        common_parking_area = pd.concat([common_parking_area, cars_area], axis=1)
        common_parking_area['Площадь МОП паркинга, в том числе:'] = np.where(
            common_parking_area['Площадь'] - common_parking_area['Площадь машино-мест'] > 0
            , common_parking_area['Площадь'] - common_parking_area['Площадь машино-мест'], 0)
        common_parking_area = common_parking_area['Площадь МОП паркинга, в том числе:']

        # Автостоянка
        commop_parking_park_area = (
            all_parts[all_parts[p.name_pn].isin(park_names)].groupby(p.section_str_pn)[p.bru_premise_part_area_pn].sum()
            .rename('Площадь'))
        commop_parking_park_area = pd.concat([commop_parking_park_area, cars_area], axis=1)
        commop_parking_park_area['Площадь автостоянки, включая рампу'] = np.where(
            commop_parking_park_area['Площадь'] - commop_parking_park_area['Площадь машино-мест'] > 0
            , commop_parking_park_area['Площадь'] - commop_parking_park_area['Площадь машино-мест'], 0)
        commop_parking_park_area = commop_parking_park_area['Площадь автостоянки, включая рампу']

        # endregion
        # region Технические помещения

        tech_count = all_parts[all_parts[p.bru_destination_pn] == 'Техническое'].groupby(
            p.section_str_pn).size().rename('Кол-во технических помещений')
        tech_area = (all_parts[all_parts[p.bru_destination_pn] == 'Техническое'].groupby(p.section_str_pn)[
                         p.bru_premise_part_area_pn]
                     .sum().rename('Площадь технических помещений'))

        # endregion

        sections = all_parts.dropna(subset=[p.section_str_pn]).sort_values(p.section_str_pn, ascending=True)[
            p.section_str_pn].unique()
        df_tep = pd.DataFrame(index=sections)
        values = [development_area
            ,below_zero_floors
            , floors_count
            , under_zero_floors
            , flats_count
            , all_one_room_flats_count
            , one_room_flats_count
            , zero_room_flats_count
            , one_free_room_flats_count
            , one_duplex_room_flats_count
            , all_two_room_flats_count
            , two_room_flats_count
            , two_duplex_room_flats_count
            , two_free_room_flats_count
            , all_three_room_flats_count
            , three_room_flats_count
            , three_duplex_room_flats_count
            , three_free_room_flats_count
            , all_four_room_flats_count
            , four_room_flats_count
            , four_duplex_room_flats_count
            , four_free_room_flats_count
            , living_flat_area_full
            , no_summer_flat_area_full
            , koef_flat_area_full
            , no_koef_flat_area_full
            , retail_count
            , retail_MC
            , pantries_all
            , pantries_indiv
            , pantries_parking
            , bicycles_count
            , cars_count
            , retail_area
            , retail_MC_area
            , pantries_all_area
            , pantries_indiv_area
            , pantries_parking_area
            , bicycles_area
            , cars_area
            , common_sect_count
            , common_parking_count
            , commop_parking_park_count
            , common_sect_area
            , common_parking_area
            , commop_parking_park_area
            , tech_count
            , tech_area]
        for df_value in values:
            df_tep = pd.concat(objs=[df_tep, df_value], axis=1)
        df_tep = df_tep.transpose()
        df_tep = df_tep.rename_axis('Наименование показателя', axis='columns')

        df_tep['Итого по объекту'] = df_tep.sum(axis=1)
        return df_tep

    def get_comparing_df_crm_bim(self,crm_path,how):
        # Загрузка данных из СРМ
        crm_df = pd.read_excel(crm_path, sheet_name='Лист1')

        # Удаляем заголовок с названием объекта
        col_names = ['Наименование'
            , 'Вид помещения'
                     # ,'Блок-секция'
                     # ,'Этаж'
                     # ,'Номер на площадке'
            , 'Блок-секция'
            , 'Этаж'
            , 'Номер на площадке'
            , 'Количество комнат'
            , 'Площадь общая'
            , 'Площадь без балкона'
            , 'Площадь балкона'
            , 'Площадь лоджии'
            , 'Площадь террасы'
            , 'Тип квартиры'
            , '(БА) Типология']
        crm_df = crm_df.drop(0, axis=0)
        crm_df = crm_df.dropna(axis=0, subset=['Код'])
        crm_df = crm_df[col_names]
        crm_df['Номер помещения'] = crm_df['Наименование'].str.extract(r"-(.*)")

        flats = self.getDfOfSellPremisesByDest('Жилье')
        rets = self.getDfOfSellPremisesByDest('Ритейл')
        pants = self.getDfOfSellPremisesByDest('Кладовки')
        parks = self.getDfOfSellPremisesByDest('Паркинг')

        bd_df = pd.concat([flats, rets], sort=False, axis=0)
        bd_df = pd.concat([bd_df, pants], sort=False, axis=0)
        bd_df = pd.concat([bd_df, parks], sort=False, axis=0)

        bd_df = bd_df.groupby('Номер помещения').apply(lambda x: x.nlargest(1, p.bru_premise_full_area_pn))
        bd_df = bd_df.rename(columns={'Площадь квартиры без коэффициентов': 'Площадь общая'
            , "Площадь без летних помещений": "Площадь без балкона"
            , 'Тип квартиры': '(БА) Типология'
            , 'BRU_Тип квартиры': 'Тип квартиры'
            , 'Секция число': 'Блок-секция'
            , 'Индекс квартиры': 'Номер на площадке'})
        bd_df['Площадь лоджии'] = bd_df['Площадь теплой лоджии'] + bd_df['Площадь холодной лоджии']
        bd_df['Этаж'] = bd_df[p.adsk_premise_number].str.split('.').str[1]

        # Для машиномест
        bd_df['Площадь общая'] = np.where(bd_df['Вид помещения'] == 'Машино-место', bd_df['Площадь помещения'],
                                          bd_df['Площадь общая'])
        bd_df['Номер на площадке'] = np.where(bd_df['Вид помещения'] == 'Машино-место', bd_df['Позиция'],
                                              bd_df['Номер на площадке'])

        # Переназначаем вид
        bd_df['Вид помещения'] = np.where(bd_df['Назначение'] == 'Жилье', 'Квартира', bd_df['Вид помещения'])
        bd_df['Вид помещения'] = np.where(bd_df['Назначение'] == 'Кладовки', 'Кладовая', bd_df['Вид помещения'])
        bd_df['Вид помещения'] = np.where(bd_df['Назначение'] == 'Паркинг', 'Паркинг', bd_df['Вид помещения'])
        bd_df['Вид помещения'] = np.where(bd_df['Назначение'] == 'Ритейл', 'Офис', bd_df['Вид помещения'])

        # Приводим к одному виду и типу
        col_names.remove('Наименование')
        col_names.append('Номер помещения')
        bd_df = bd_df[col_names]
        bd_df = bd_df.apply(p.convert_to_double)

        # Сопоставление
        # comp_df = pd.merge(left=crm_df,right=bd_df,how='left',on='Номер помещения',suffixes=('_CRM','_BIM'))
        comp_df = pd.merge(left=crm_df, right=bd_df, how=how,
                           on=['Вид помещения', 'Блок-секция', 'Этаж', 'Номер на площадке'], suffixes=('_CRM', '_BIM'))
        comp_df = comp_df.fillna(0)
        comp_df['Площадь общая_Δ'] = comp_df['Площадь общая_CRM'] - comp_df['Площадь общая_BIM']
        comp_df['Площадь без балкона_Δ'] = comp_df['Площадь без балкона_CRM'] - comp_df['Площадь без балкона_BIM']
        comp_df['Площадь балкона_Δ'] = comp_df['Площадь балкона_CRM'] - comp_df['Площадь балкона_BIM']
        comp_df['Площадь лоджии_Δ'] = comp_df['Площадь лоджии_CRM'] - comp_df['Площадь лоджии_BIM']
        comp_df['Площадь террасы_Δ'] = comp_df['Площадь террасы_CRM'] - comp_df['Площадь террасы_BIM']
        comp_df = comp_df[['Номер помещения_CRM', 'Номер помещения_BIM', 'Вид помещения'
            , 'Площадь общая_CRM', 'Площадь общая_BIM', 'Площадь общая_Δ']]
        return comp_df
    
    def find_doubles(self,bru_type):
        prems = self.fullDf
        prems = prems[prems[p.type_pn].isin([bru_type])]
        # prems[p.adsk_index_int_pn] = np.where(prems[p.adsk_index_int_pn] == 0,0,prems[p.adsk_index_int_pn])

        param = p.adsk_premise_number
        grPrems = prems.groupby([p.bru_category_pn,p.bru_section_int_pn,p.bru_floor_int_pn,p.adsk_index_int_pn],as_index=False)[param].apply(lambda x: set(x))
        grPrems = grPrems[grPrems[param].apply(lambda x: len(x)) > 1]
        return grPrems
    
    def show_floor_types(self):
        """Метод получения списка секций и этажей, а также их типов (определяется исходя из назначения помещений)

        Returns
        -------
            Датафрейм со значениями секций, этажей и типов этажей
        """
        df = self.fullDf
        sect_fl_df = df.groupby([p.section_str_pn,p.bru_section_int_pn,p.bru_floor_int_pn],as_index=False)[p.bru_destination_pn].agg(lambda x: ','.join(set(x)))
        sect_fl_df['Тип этажа'] = ""
        sect_fl_df['Тип этажа'] = np.where((sect_fl_df['Тип этажа'] == '') & (sect_fl_df[p.bru_floor_int_pn] < 1)
                                        ,'-1 этаж'
                                        ,'')
        sect_fl_df['Тип этажа'] = np.where((sect_fl_df['Тип этажа'] == '') & (sect_fl_df[p.bru_floor_int_pn] == 1)
                                        ,'1 этаж'
                                        ,sect_fl_df['Тип этажа'])
        sect_fl_df['Тип этажа'] = np.where(
                                            (sect_fl_df['Тип этажа'] == '') & 
                                            sect_fl_df[p.bru_destination_pn].str.contains('Жилье') & 
                                            sect_fl_df[p.bru_destination_pn].str.contains('Техническое'),
                                            'Верхний этаж',
                                            sect_fl_df['Тип этажа']
                                        )

        sect_fl_df['Тип этажа'] = np.where(
                                            (sect_fl_df['Тип этажа'] == '') & 
                                            sect_fl_df[p.bru_destination_pn].str.contains('Техническое'),
                                            'Верхний этаж',
                                            sect_fl_df['Тип этажа']
                                        )
        sect_fl_df['Тип этажа'] = np.where(
                                            (sect_fl_df['Тип этажа'] == '') & 
                                            sect_fl_df[p.bru_floor_int_pn] != 0,
                                            'Типовой этаж',
                                            sect_fl_df['Тип этажа']
                                        )
        sect_fl_df = sect_fl_df.drop('Назначение',axis=1)
        return sect_fl_df

