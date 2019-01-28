import requests
import json
import pandas as pd
import datetime
import re

def get_json():
    url = 'http://incrm.ru/export-tred/ExportToSite.svc/ExportToTf/json'
    r = requests.get(url)
    json_data=json.loads(r.text)
    data_frame = pd.DataFrame.from_records(json_data,columns = ["ArticleID", "Article", "Number", "StatusCode", "StatusCodeName", "Quantity", "Rooms", "Sum",
                       "Finishing", "Decoration", "SeparateEntrance","RoofExit","2level","TerrasesCount"])
    print('JSON застройщика успешно прочитан')
    return data_frame

def maintain_df(data_frame):
    data_frame = data_frame.rename(
        columns={'Article': 'Код объекта','Number': 'Номер квартиры', 'StatusCodeName': 'Статус',
                 'Quantity': 'Площадь',
                 'Sum': 'Цена', 'Decoration': 'Отделка'})
    data_frame = data_frame.assign(domain=data_frame['Код объекта'])
    data_frame = data_frame[data_frame['domain'].str.contains('ОБ')]
    data_frame = data_frame.drop(
        columns=['ArticleID', 'Rooms', 'StatusCode', 'Finishing', 'SeparateEntrance', 'RoofExit', '2level',
                 'TerrasesCount', 'domain'])
    data_frame['Цена за метр'] = data_frame['Цена'].astype(float) / data_frame['Площадь'].astype(float)
    data_frame['Цена'] = data_frame['Цена'].astype(float)
    data_frame['Площадь'] = data_frame['Площадь'].astype(float)
    return data_frame

def mer(data_frame):
    data_ob = pd.read_excel('\\\\192.168.10.123\\аналитика\\Отчеты\\Сверка Васильев\\obl.xlsx', usecols=[5,9,10,11,13,16,23,18,19])
    print('Файл Васильева успешно прочитан')
    merge_df_1 = pd.merge(data_ob, data_frame, how='left', on='Код объекта')
    merge_df_1.rename(columns={'Комнат. Студия=0':'Комнат'},inplace=True)
    merge_df_1.rename(columns={'Дата создания (договора) (Клиентский договор (оптовый)) (Договор (сделка))':'Дата договора'}, inplace=True)
    merge_df_1['Дата договора'] = pd.to_datetime(merge_df_1['Дата договора']).apply(lambda x:x.date())
    merge_df_1['Комнат'].replace({0:'CT',1:'1K',2:'2K',3:'3K',4:'4K'},inplace=True)
    merge_df_1 = merge_df_1.replace(
        {'без отделки': 0, 'чистовая МП': 2, 'Классика': 2, 'МОДЕРН': 2, 'СОЧИ': 2,
         'Финишная отделка': 2, 'ч/о без перегородок': 1, 'черновая': 1, 'чистовая': 2, 'чистовая (светлая)': 2,
         'чистовая (темная)': 2, 'ЯЛТА': 2, 'Без отделки': 0, 'Модерн': 2, 'Сочи': 2, 'Ялта': 2, 'Чистовая': 2,
         'Черновая': 1,
         'без отделки (old)': 0, 'Венеция': 2, 'венеция': 2, 'ВЕНЕЦИЯ': 2, '': 0, "": 0})
    for i in range(len(merge_df_1)):
        if (pd.isnull(merge_df_1.loc[i, 'Цена']) and pd.notnull(merge_df_1.loc[i, 'Сумма сделки (Заявка устной брони) (Заявка)'])):
            merge_df_1.loc[i,'Цена'] = float(merge_df_1.loc[i, 'Сумма сделки (Заявка устной брони) (Заявка)'])
        elif (pd.isnull(merge_df_1.loc[i,'Цена']) and pd.notnull(merge_df_1.loc[i,'Стоимость продажи'])):
            merge_df_1.loc[i, 'Цена'] = float(merge_df_1.loc[i,'Стоимость продажи'])
        if(pd.isnull(merge_df_1.loc[i,'Площадь']) and pd.notnull(merge_df_1.loc[i,'Количество'])):
            merge_df_1.loc[i, 'Площадь'] = float(merge_df_1.loc[i,'Количество'])
        if(pd.isnull(merge_df_1.loc[i,'Статус'])):
            merge_df_1.loc[i,'Статус'] = merge_df_1.loc[i,'Состояние объекта']
        if(pd.isnull(merge_df_1.loc[i,'Отделка_y'])):
            merge_df_1.loc[i, 'Отделка_y'] = merge_df_1.loc[i,'Отделка_x']
        if(pd.isnull(merge_df_1.loc[i,'Цена за метр'])):
            merge_df_1.loc[i, 'Цена за метр'] = merge_df_1.loc[i,'Цена'] / merge_df_1.loc[i,'Площадь']
    merge_df_1['Доступность к продаже'] = merge_df_1['Статус']
    merge_df_1['Цена за метр'] = merge_df_1['Цена за метр'].round(2)
    merge_df_1.replace({'Доступность к продаже': {'Оценка': 3, 'Ус. Бронь': 1, 'Продажа': 0, 'Свободно': 1,
                                                  'Стр. Резерв': 3, 'Пл. Бронь': 2}},inplace=True)
    merge_df_1.drop(columns=['Стоимость продажи','Отделка_x','Сумма сделки (Заявка устной брони) (Заявка)','Номер квартиры','Количество','Статус'])
    data_site_flats = pd.read_excel('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\zhk_oblaka_.xlsx',sheet_name=0)
    data_site_aparts = pd.read_excel('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\zhk_oblaka_.xlsx',sheet_name=1)
    df2 = pd.merge(merge_df_1[merge_df_1['Код объекта'].str.contains('ОБ-КВ')],data_site_flats,how='left',on='Условный номер')
    df2.to_csv('DF2.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    df2['площадь        ']=df2['Площадь']
    df2['Доступность к продаже_y'] = df2['Доступность к продаже_x']
    df2['Стоимость'] = df2['Цена']
    df2['Отделка'] = df2['Отделка_y']
    df_aparts = pd.merge(merge_df_1[merge_df_1['Код объекта'].str.contains('ОБ-АП')],data_site_aparts,how='left',on='Условный номер')
    df_aparts.to_csv('DF3.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    data_site_aparts['площадь        ']=df_aparts['Площадь']
    data_site_aparts['Доступность к продаже_y'] = df_aparts['Доступность к продаже_x']
    data_site_aparts['Стоимость'] = df_aparts['Цена']
    data_site_aparts['Отделка'] = df_aparts['Отделка_y']
    for i in range(len(merge_df_1)):
        merge_df_1.loc[i, 'Стояк'] = int(re.search('\d\d', re.search('-\d\d-\d\d\d', merge_df_1.loc[i, 'Код объекта']).group(0)).group(0))
        merge_df_1.loc[i, 'Секция'] = int(re.search('\d+', merge_df_1.loc[i, 'Код объекта']).group(0))
    df2.rename(columns={'Доступность к продаже_y':'Доступность к продаже','Комнат_y':'Комнат'},inplace=True)

    writer = pd.ExcelWriter('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\zhk_oblaka_.xlsx')
    df2.to_excel(writer, '1',
                 columns=['Корпус', 'Подъезд', 'ЭТАЖ', 'Условный номер', 'Номер квартиры на этаже', 'Комнат',
                          'площадь        ', 'Доступность к продаже', 'Стоимость', 'Отделка', 'тэг'], index=False)
    data_site_aparts.to_excel(writer, '2',columns=['Корпус', 'Подъезд', 'ЭТАЖ', 'Условный номер', 'Номер квартиры на этаже','Комнат', 'площадь        ', 'Доступность к продаже', 'Стоимость', 'Отделка','тэг'], index=False)

    writer.save()
    print('Загрузочный файл для сайта сформирован')
    writer = pd.ExcelWriter('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\Облака прайс.xlsx')
    merge_df_1.to_excel(writer,'1',columns=['Код объекта','Секция','Стояк','Условный номер','Площадь','Комнат','Доступность к продаже','Цена','Цена за метр','Отделка_y','Дата договора'],index=False)
    writer.save()
    print('Прайс для 1С сформирован')
    return merge_df_1
def compare_df(new_df):
    old_df = pd.read_excel('Итоги '+(datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")+'.xlsx', usecols=[0,1,2,3,4,5])
    data = pd.merge(old_df,new_df, how='left', on='Код объекта')
    data['Площадь_отличия'] = data['Площадь_x'] - data['Площадь_y']
    data['Разница'] = data['Цена_x'] - data['Цена_y']
    data['Отделка_отличия'] = data['Отделка_x'] - data['Отделка_y']
    data['Статус_отличия']=""
    for i in range (len(data)):
        data.loc[i,'Стояк'] = int(re.search('\d\d', re.search('-\d\d-\d\d\d', data.loc[i,'Код объекта']).group(0)).group(0))
        if (data.loc[i, 'Площадь_x'] != data.loc[i, 'Площадь_y']):
            data.loc[i, 'Статус_отличия'] = "Изменение площади на " + str(data.loc[i, 'Площадь_x'] - data.loc[i, 'Площадь_y'])
        if (data.loc[i, 'Цена_x'] != data.loc[i, 'Цена_y'] and pd.notnull(data.loc[i,'Цена_x']) and pd.notnull(data.loc[i,'Цена_y'])):
            data.loc[i, 'Статус_отличия'] = str(data.loc[i, 'Статус_отличия']) + "Изменение цены на " + str(int(data.loc[i, 'Цена_x'] - data.loc[i, 'Цена_y'])) + ' '
        if (data.loc[i, 'Отделка_x'] != data.loc[i, 'Отделка_y']):
            data.loc[i, 'Статус_отличия'] = str(data.loc[i, 'Статус_отличия']) + "Изменение отделки на " + str(data.loc[i, 'Отделка_x'])
        if (data.loc[i, 'Статус_x'] != data.loc[i, 'Статус_y']):
            data.loc[i, 'Статус_отличия'] = str(data.loc[i, 'Статус_отличия']) + "Изменение статуса на " + str(data.loc[i, 'Статус_x']) + "(было " + str(data.loc[i, 'Статус_y']) + ")"
    data2 = data.loc[(data['Статус_отличия']!="")]
    writer = pd.ExcelWriter('Otliciya ' + datetime.date.today().strftime("%Y-%m-%d") + '.xlsx')
    data2 = data2.rename(
        columns={'Цена_x': 'Цена стало', 'Цена_y': 'Цена было', 'Статус_x': 'Статус стало', 'Статус_y': 'Статус было',
                 'Условный номер_x': 'Условный номер'})
    data2.to_excel(writer, columns=['Код объекта','Стояк','Условный номер','Статус_отличия','Цена стало','Цена было','Разница'],index=False,float_format='%.2f')
    writer.save()
    print('Файл с отличиями сформирован')

def sverka(oblaka_price):
    oblaka_price.drop(columns=['Дата договора','Количество','Стоимость продажи','Состояние объекта','Сумма сделки (Заявка устной брони) (Заявка)','Номер квартиры','Статус','Отделка_x'],inplace=True)
    grishin_price = pd.read_excel('grishin_price.xlsx', usecols=[0, 8, 10, 9])
    grishin_price = grishin_price.rename(
        columns={'Стоимость продажи': 'Grishin_price', 'Отделка': 'Grishin_decoration',
                 'Вывод в продажу 1/0': 'Grishin_status'})
    grishin_price['Grishin_decoration'].replace(['\w/\w', '\wернов*', '\wистов*'], [0, 1, 2], inplace=True, regex=True)
    check = pd.merge(oblaka_price, grishin_price, how='inner', on='Код объекта')
    for i in range(len(check)):
        if (pd.notnull(check.loc[i, 'Цена'])):
            check.loc[i, 'Price_differ'] = round(check.loc[i, 'Grishin_price'] - check.loc[i, 'Цена'], 0)
        check.loc[i, 'Status_differ'] = check.loc[i, 'Grishin_status'] - check.loc[i, 'Доступность к продаже']
        check.loc[i, 'Decoration_differ'] = check.loc[i, 'Grishin_decoration'] - check.loc[i, 'Отделка_y']
    check = check[check['Price_differ'].notnull()]
    check = check[
        (abs(check['Price_differ']) > 1) | (abs(check['Status_differ']) > 0) | (abs(check['Decoration_differ']) > 0)]
    writer = pd.ExcelWriter('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\Сверка Облаков.xlsx')
    check.to_excel(writer, '1', columns=['Код объекта', 'Секция', 'Стояк', 'Условный номер', 'Площадь', 'Комнат',
                                         'Доступность к продаже', 'Цена', 'Цена за метр', 'Отделка_y', 'Grishin_price',
                                         'Price_differ', 'Grishin_status', 'Status_differ', 'Decoration_differ'],
                   index=False)
    writer.save()
    print('Сверка сформирована')

if __name__ == '__main__':
    try:
        data = get_json()  # берём данные из CRM застройщика и перобразуем их в DataFrame
        data = maintain_df(
            data)  # обрабатываем DataFrame (выбираем только Облака, преобразуем данные в float и отсеиваем лишние колонки)
        data = mer(data)  # прводим "левое" слияние с выгрузкой Васильева
        #sverka(data)
        #compare_df(data)
        print('Всё готово!')
        input('Для продолжения нажми Enter')
    except PermissionError:
        print('Ошибка! Закрой открытые файлы')
        input('Для продолжения нажми Enter')
        pass


