import numpy as np
import re
import sys

import pandas as pd


class ParamsAndFuns:
    NamePN = 'Имя'
    BruDestinationPn = 'Назначение'
    TypePn = 'Вид помещения'
    BruCategoryPn = 'Категория'

    AdskPremiseNumber = 'Номер квартиры'
    PremisePartNumber = 'Номер части помещения'
    SectionStrPN = 'Номер секции'
    BruSectionIntPN = 'Секция число'
    BruFloorIntPN = 'Этаж'
    ADSKIndexIntPN = 'Индекс квартиры'
    ADSKPositionIntPN = 'ADSK_Позиция'

    AdskTypePn = 'Тип квартиры'
    BruTypePn = 'BRU_Тип квартиры'

    BruPremiseNumberPN = 'BRU_Номер помещения'

    BruPremisePartAreaPN = 'Площадь помещения'
    BruPremiseFullAreaPN = 'Площадь квартиры без коэффициентов'
    BruPremiseNonSummerAreaPn = 'Площадь без летних помещений'
    BruPremiseSummerAreaPn = 'Площадь летних'
    BruPremiseLivingAreaPN = 'Площадь квартиры жилая'

    OnThreeAndMoreSides = 'На три и более сторон'
    OnThreeSides = "Торцевая (три стороны)"


    HasTerraseOnRoof = 'Терраса на кровле'
    HasTerraseOnFloor = 'Терраса на земле'
    HasBalcony = 'Балкон'
    HasColdLoggia = 'Лоджия (холодная)'
    HasWarmLoggia = 'Лоджия (теплая)'

    SummerAreaNames = ['Балкон','Лоджия','Лоджия (холодная)','Терраса на земле','Терраса']

    RetailElevList = ['Одноуровневое на 1 этаже','Одноуровневое на 2 этаже','Одноуровневое на -1 этаже'
                      ,'Многоуровневое -1 этаж до 30проц','Многоуровневое -1 этаж более 30проц','Многоуровневое 2 этаж до 50проц'
                      ,'Многоуровневое 2 этаж более 50проц']

    #Окна
    PremiseNumberToPN = 'Номер помещения To'
    PremiseNumberFromPN = 'Номер помещения From'
    PremisePartNumberToPN = 'Номер части помещения To'
    PremisePartNumberFromPN = 'Номер части помещения From'
    PremisePartNameToPN = 'Имя помещения To'
    PremisePartNameFromPN = 'Имя помещения From'
    OrientSidePN = 'Сторона света'
    # WindowDest = 'BRU_Направление'

    windowColums = [
        # 'Комментарии',
        BruDestinationPn,
        PremiseNumberToPN,
        PremisePartNumberToPN,
        PremiseNumberFromPN,
        PremisePartNumberFromPN,
        # PremisePartNameFromPN,
        # PremisePartNameToPN,
        OrientSidePN,
        # WindowDest
    ]

    def set_np_pd_opts():
        desired_width = 340
        pd.set_option('display.width', desired_width)
        np.set_printoptions(linewidth=desired_width)
        pd.set_option('display.max_columns', 10)

    def convertToDouble(value):
        try:
            return value.astype(float)
        except:
            return value

    def defineTechEquipment(roomName):
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

    def getUnique(someStr):
        someList = str(someStr).split(sep=',')
        unique = list(set(someList))
        return ','.join(unique)

    def countItems(someStr):
        someList = str(someStr).split(sep=',')
        return len(someList)

    def show_columns_with_word(df,word):
        res = df.columns[df.columns.str.contains(word,False) == True]
        return res

    def regex_floor_name(floor):
        if (floor is np.nan):
            return 'NaN'
        pattern = r'Этаж\s(-?\d+)'
        match = re.search(pattern, floor)
        if (match):
            res = match.group()
        else:
            res = 'NaN'
        return res

    def show_stat_info(df,start_t,fin_t):
        full_time = fin_t - start_t
        mbite = round(sys.getsizeof(df) * (0.125*(10**(-6))),2)
        print(f'Затраченное время: {full_time}, вес: {mbite} МБайт')



