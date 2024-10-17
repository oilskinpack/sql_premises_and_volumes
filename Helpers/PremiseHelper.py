import numpy as np
import pandas as pd
from Helpers.ParamsAndFuns import ParamsAndFuns as p


class PremiseHelper:
    def __init__(self,fullPath):
        """
        Конструктор класса, где нужно указать путь к csv файлу,для загрузки DF по помещениям

        Parameters
        -------
        fullPath: str
            Путь к файлу формата r'D:\Khabarov\RVT\Premises\TEP'\test.csv, разделитель ';'
        """
        dfParamValues = pd.read_csv(fullPath, sep=';')
        self.fullDf = self.transposePremiseDf(dfParamValues)

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
            destDf = fullDf[(fullDf[p.BruDestinationPn] == destination) & (fullDf[p.BruTypePn] != 'МОП')]
        else:
            destDf = fullDf[(fullDf[p.TypePn] == 'Машино-место')]
        # Превращаем в инты основные параметры
        destDf = destDf.apply(p.convertToDouble)
        if destination != 'Паркинг':
            destDf = destDf.sort_values([p.BruFloorIntPN, p.BruSectionIntPN, p.ADSKIndexIntPN])
        else:
            destDf = destDf.sort_values([p.BruFloorIntPN, p.BruFloorIntPN, p.ADSKIndexIntPN])
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
        flatDfGrouped = destDf.groupby(p.AdskPremiseNumber).min(numeric_only=True)
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
            sum = self.getDfOfSellPremisesByDest(destination)[p.BruPremisePartAreaPN].sum()
        else:
            sum = self.getDfOfSellPremisesByDestGrouped(destination)[p.BruPremiseFullAreaPN].sum()
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
            matrix = df.groupby([p.AdskTypePn, p.AdskPremiseNumber]).size().groupby(p.AdskTypePn).size()
        else:
            matrix = df.groupby([p.BruTypePn, p.AdskPremiseNumber]).size().groupby(p.BruTypePn).size()
        return matrix

    def getCommonPremisesDf(self):
        """
        Метод получение датафрейма по помещениям МОП объекта, по виду как для ДОМ.рф

        Returns: pd.DataFrame
            Датафрейм по формату ДОМ.рф для МОП

        """
        fullDf = self.fullDf
        mopDf = fullDf[fullDf[p.TypePn] == 'МОП'].sort_values(
            [p.BruSectionIntPN, p.BruFloorIntPN, p.AdskPremiseNumber])[
            [p.NamePN, p.BruPremisePartAreaPN, p.BruSectionIntPN, p.BruFloorIntPN]]

        mopDf['Назначение помещения'] = 'Общественное'
        mopDf['Описание местоположения помещения'] = 'Секция ' + mopDf[p.BruSectionIntPN]
        mopDf = mopDf.rename(columns={p.NamePN: 'Вид помещения'})
        mopDf = mopDf.rename(columns={p.BruPremisePartAreaPN: 'Площадь,м²'})
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
        lk_df = common_df[common_df[p.NamePN].str.contains('лестничная',False) == True]
        max_lk = lk_df.groupby(p.BruSectionIntPN).max(numeric_only = True).reset_index()
        max_lk ['Имя'] = 'Вертикальный транспорт'
        max_lk = max_lk[[p.NamePN,p.BruPremisePartAreaPN, p.BruSectionIntPN, p.BruFloorIntPN]].reset_index()
        max_lk = max_lk.apply(p.convertToDouble)

        fullDf = self.fullDf
        techDf = fullDf[fullDf[p.BruDestinationPn] == 'Техническое'].sort_values(
            [p.BruSectionIntPN, p.BruFloorIntPN, p.AdskPremiseNumber])[
            [p.NamePN, p.BruPremisePartAreaPN, p.BruSectionIntPN, p.BruFloorIntPN]].reset_index()
        techDf = techDf.apply(p.convertToDouble)

        techDf = pd.concat([techDf,max_lk],axis=0)
        techDf = techDf.sort_values([p.BruSectionIntPN,p.BruFloorIntPN],ascending=[True,True])

        techDf[p.BruSectionIntPN] = techDf[p.BruSectionIntPN].astype(str).str.split('.').str[0]
        techDf['Описание местоположения помещения'] = ''
        techDf['Часть'] = ''
        techDf = techDf.rename(columns={p.NamePN: 'Назначение'})
        techDf['Вид оборудования'] = techDf['Назначение'].apply(p.defineTechEquipment)
        techDf['Вид оборудования'] = np.where(((techDf['Вид оборудования'] == 'Лифт') & (techDf[p.BruFloorIntPN] < 15)),'Лифт, 2 шт.',techDf['Вид оборудования'])
        techDf['Вид оборудования'] = np.where(((techDf['Вид оборудования'] == 'Лифт') & (techDf[p.BruFloorIntPN] >= 15)),'Лифт, 4 шт.',techDf['Вид оборудования'])


        techDf['Часть'] = np.where(techDf['Вид оборудования'].str.contains('электро') == True,'Подземная часть, ',techDf['Часть'])
        techDf['Часть'] = np.where(techDf['Вид оборудования'].str.contains('Лифт') == True,'',techDf['Часть'])
        techDf['Часть'] = np.where(techDf['Вид оборудования'] == '','Подземная и надземная части, ',techDf['Часть'])
        techDf['Описание местоположения помещения'] = techDf['Часть']+ 'Секция ' + techDf[p.BruSectionIntPN].astype(str)
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
        flatsDf[(flatsDf[p.NamePN].str.contains('Терраса') == False) & (flatsDf[p.NamePN].isin(p.SummerAreaNames))][
            p.BruPremisePartAreaPN].sum()
        terraseArea = flatsDf[(flatsDf[p.NamePN].str.contains('Терраса') == True) & (flatsDf[p.NamePN].isin(p.SummerAreaNames))][
            p.BruPremisePartAreaPN].sum()
        retailBelowZeroArea = retailDf[retailDf[p.AdskPremiseNumber].str.contains('.-') == True][
            p.BruPremisePartAreaPN].sum()
        carsBelowZeroArea = carsDf[carsDf[p.BruFloorIntPN] <= -1][p.BruPremisePartAreaPN].sum()
        pantriesBelowZeroArea = pantriesDf[pantriesDf[p.BruFloorIntPN] <= -1][p.BruPremisePartAreaPN].sum()

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
        fullDf = self.fullDf.apply(p.convertToDouble)
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

        loggiaArea = flatsDf[flatsDf[p.NamePN].str.contains('Лоджия') == True][p.BruPremisePartAreaPN].sum()
        terraseOnRoofArea = flatsDf[flatsDf[p.NamePN] == 'Терраса'][p.BruPremisePartAreaPN].sum()
        terraseOnGroundArea = flatsDf[flatsDf[p.NamePN] == 'Терраса на земле'][p.BruPremisePartAreaPN].sum()
        balconyArea = flatsDf[flatsDf[p.NamePN].str.contains('Балкон') == True][p.BruPremisePartAreaPN].sum()
        mopArea = round(fullDf[fullDf[p.TypePn] == 'МОП'].apply(p.convertToDouble)[p.BruPremisePartAreaPN].sum(),2)
        techArea = round(fullDf[fullDf[p.TypePn] == 'Технические помещения'].apply(p.convertToDouble)[p.BruPremisePartAreaPN].sum(),2)
        typicalFloorArea = fullDf[(fullDf[p.BruFloorIntPN] == 3) & (fullDf[p.BruCategoryPn].notnull())][
            p.BruPremisePartAreaPN].sum()
        typicalSaleArea = fullDf[(fullDf[p.BruFloorIntPN] == 3) & (fullDf[p.BruCategoryPn].notnull()) & (
                    fullDf[p.TypePn] != 'МОП')][p.BruPremisePartAreaPN].sum()
        fullParkingArea = \
        fullDf[(fullDf[p.BruCategoryPn] == 'Паркинг') & (fullDf[p.NamePN].str.contains('Автостоянк'))][
            p.BruPremisePartAreaPN].sum()
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
        fullDf = self.fullDf
        windowsDf = fullDf[
            (fullDf[p.BruDestinationPn] == 'Окно') | (fullDf[p.BruDestinationPn] == 'Витраж')]

        windowsFlats = windowsDf[(windowsDf['Номер части помещения To'].str.contains('Ж') == True) | (
                    windowsDf['Номер части помещения From'].str.contains('Ж') == True)]
        windowsFlats = windowsFlats[windowsFlats[p.BruDestinationPn] != 'Витраж']
        windowsFlats = windowsFlats[p.windowColums]

        return windowsFlats

    def getWindowFlatsOrienDf(self):
        windowsFlats = self.getWindowsDf()
        windowsFlats['Номер помещения'] = np.where(windowsFlats['Номер помещения To'] is np.nan,
                                                   windowsFlats['Номер помещения To'],
                                                   windowsFlats['Номер помещения From'])
        windowsFlats = windowsFlats.groupby('Номер помещения', as_index=False)['Сторона света'].agg(','.join)
        windowsFlats['Уникальные стороны'] = windowsFlats.reset_index()['Сторона света'].apply(p.getUnique)
        windowsFlats['Кол-во сторон'] = windowsFlats['Уникальные стороны'].apply(p.countItems)
        return windowsFlats

    def getGnsDf(self):
        fullDf = self.fullDf
        gnsDf = fullDf[fullDf[p.BruDestinationPn] == 'ГНС']
        gnsGrouped = gnsDf[[p.SectionStrPN, p.BruFloorIntPN, p.BruPremisePartAreaPN]].groupby(
            [p.SectionStrPN, p.BruFloorIntPN]).sum()
        return gnsGrouped

    def get_flats_with_summer_premises(self):
        flatsDf = self.getDfOfSellPremisesByDest('Жилье')
        flatsDf  = flatsDf [flatsDf [p.BruPremiseSummerAreaPn] > 0]
        flatsWithSummerPremises = flatsDf[
            [p.HasTerraseOnRoof, p.HasTerraseOnFloor, p.HasBalcony, p.HasColdLoggia, p.HasWarmLoggia,
             p.BruPremiseSummerAreaPn]]
        return flatsWithSummerPremises

    def duplex_flats(self):
        flatPrem = self.getDfOfSellPremisesByDest('Жилье')
        flats_levels_count_df = (flatPrem.groupby([p.AdskPremiseNumber, p.BruFloorIntPN]).size().reset_index()
                                 .groupby([p.AdskPremiseNumber]).size())
        res = flats_levels_count_df[flats_levels_count_df > 1]
        return res
