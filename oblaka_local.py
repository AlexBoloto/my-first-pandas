import requests
import json
import pandas as pd
import datetime
import re

def get_json(): 
    url = 'http://incrm.ru/export-tred/ExportToSite.svc/ExportToTf/json' #адрес JSON-данных застройщика по всем ЖК. Данные только по свободным квартирам.
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
    data_frame = data_frame[data_frame['domain'].str.contains('ОБ')] #Выбираем данные только по ЖК Облака. 
    data_frame = data_frame.drop(
        columns=['ArticleID', 'Rooms', 'StatusCode', 'Finishing', 'SeparateEntrance', 'RoofExit', '2level',
                 'TerrasesCount', 'domain']) #Отбрасываем колонки, которые пока не будут использоваться
    data_frame['Цена за метр'] = data_frame['Цена'].astype(float) / data_frame['Площадь'].astype(float) #вычисляем цену за кв. метр
    data_frame['Цена'] = data_frame['Цена'].astype(float) #Преобразуем данные во float
    data_frame['Площадь'] = data_frame['Площадь'].astype(float)
    return data_frame

def merge_df(data_frame):
    data_ob = pd.read_excel('obl.xlsx', usecols=[5,9,10,11,13,16,23,18,19]) #читаем файл застройщика, берем только некоторые колонки
    print('Файл сверки застройщика успешно прочитан')
    merge_df_1 = pd.merge(data_ob, data_frame, how='left', on='Код объекта')#проводим слияние по уникальному Коду объекта
    merge_df_1.rename(columns={'Комнат. Студия=0':'Комнат','Дата создания (договора) (Клиентский договор (оптовый)) (Договор (сделка))':'Дата договора'},inplace=True) #переименовываем данные для удобства
    merge_df_1['Дата договора'] = pd.to_datetime(merge_df_1['Дата договора']).apply(lambda x:x.date()) #из timestamp берем только дату
    merge_df_1['Комнат'].replace({0:'CT',1:'1K',2:'2K',3:'3K',4:'4K'},inplace=True) #меняем цифровые обознанчения комнатности на буквенные
    merge_df_1 = merge_df_1.replace(
        {'без отделки': 0, 'чистовая МП': 2, 'Классика': 2, 'МОДЕРН': 2, 'СОЧИ': 2,
         'Финишная отделка': 2, 'ч/о без перегородок': 1, 'черновая': 1, 'чистовая': 2, 'чистовая (светлая)': 2,
         'чистовая (темная)': 2, 'ЯЛТА': 2, 'Без отделки': 0, 'Модерн': 2, 'Сочи': 2, 'Ялта': 2, 'Чистовая': 2,
         'Черновая': 1,
         'без отделки (old)': 0, 'Венеция': 2, 'венеция': 2, 'ВЕНЕЦИЯ': 2, '': 0, "": 0}) # Меняем буквеную отделку на цифровую (да, ужасно неоптимально, можно лучше)
    for i in range(len(merge_df_1)): # Если в сверке застройщика есть данные по квартире, то заменяем на данные из сверки, в противном случае оставляем данные из JSON
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
    merge_df_1['Цена за метр'] = merge_df_1['Цена за метр'].round(2) # Округляем до 2 знаков после запятой иначе 1С не примет
    merge_df_1.replace({'Доступность к продаже': {'Оценка': 3, 'Ус. Бронь': 1, 'Продажа': 0, 'Свободно': 1,
                                                  'Стр. Резерв': 3, 'Пл. Бронь': 2}},inplace=True) # Меняем буквеные статусы на цифровые
    merge_df_1.drop(columns=['Стоимость продажи','Отделка_x','Сумма сделки (Заявка устной брони) (Заявка)','Номер квартиры','Количество','Статус'])
    data_site_flats = pd.read_excel('zhk_oblaka.xlsx',sheet_name=0) # Читаем загрузочный файл на сайт. 1 лист - квартитры, 2 - апартаменты
    data_site_aparts = pd.read_excel('zhk_oblaka.xlsx',sheet_name=1)
    data_flats = pd.merge(merge_df_1[merge_df_1['Код объекта'].str.contains('ОБ-КВ')],data_site_flats,how='left',on='Условный номер') # Важно отделить квартиры от апартаментов, у них могут быть одинаковые номера квартиры
    data_flats['площадь        ']=data_flats['Площадь']
    data_flats['Доступность к продаже_x'] = data_flats['Доступность к продаже_y']
    data_flats['Стоимость'] = data_flats['Цена']
    data_flats['Отделка'] = data_flats['Отделка_y']
    df_aparts = merge_df_1[merge_df_1['Код объекта'].str.contains('ОБ-АП')] # Отделяем апртаменты
    data_site_aparts['площадь        ']=df_aparts['Площадь']
    data_site_aparts['Доступность к продаже'] = df_aparts['Доступность к продаже']
    data_site_aparts['Стоимость'] = df_aparts['Цена']
    data_site_aparts['Отделка'] = df_aparts['Отделка_y']
    for i in range(len(merge_df_1)):
        merge_df_1.loc[i, 'Стояк'] = int(re.search('\d\d', re.search('-\d\d-\d\d\d', merge_df_1.loc[i, 'Код объекта']).group(0)).group(0)) # Отдельно выделяем стояк и секцию по заказу рукводства
        merge_df_1.loc[i, 'Секция'] = int(re.search('\d+', merge_df_1.loc[i, 'Код объекта']).group(0))
    data_flats.rename(columns={'Доступность к продаже_y':'Доступность к продаже','Комнат_y':'Комнат'},inplace=True)
    writer = pd.ExcelWriter('zhk_oblaka.xlsx') # Пишем в загрузочный файл для сайта новые данные
    data_flats.to_excel(writer, '1',
                 columns=['Корпус', 'Подъезд', 'ЭТАЖ', 'Условный номер', 'Номер квартиры на этаже', 'Комнат',
                          'площадь        ', 'Доступность к продаже', 'Стоимость', 'Отделка', 'тэг'], index=False)
    data_site_aparts.to_excel(writer, '2',columns=['Корпус', 'Подъезд', 'ЭТАЖ', 'Условный номер', 'Номер квартиры на этаже','Комнат', 'площадь        ', 'Доступность к продаже', 'Стоимость', 'Отделка','тэг'], index=False)
    writer.save()
    print('Загрузочный файл для сайта сформирован')
    writer = pd.ExcelWriter('Oblaka_price.xlsx') # Пишем в загрузочный файл для 1С новые данные
    merge_df_1.to_excel(writer,'1',columns=['Код объекта','Секция','Стояк','Условный номер','Площадь','Комнат','Доступность к продаже','Цена','Цена за метр','Отделка_y','Дата договора'],index=False)
    writer.save()
    print('Прайс для 1С сформирован')
    return merge_df_1
def compare_df(new_df): # Смотрим а что же изменилось по сравнению с вчерашними данными?
    old_df = pd.read_excel('Summary 2019-01-15.xlsx', usecols=[0,1,2,3,4,5]) # Берем вчерашние данные
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
    writer = pd.ExcelWriter('Otliciya ' + datetime.date.today().strftime("%Y-%m-%d") + '.xlsx') #Формируем файл с отличиями
    data2 = data2.rename(columns={'Цена_x':'Цена стало','Цена_y':'Цена было','Статус_x':'Статус стало','Статус_y':'Статус было','Условный номер_x':'Условный номер'})
    data2.to_excel(writer, columns=['Код объекта','Стояк','Условный номер','Статус_отличия','Цена стало','Цена было','Разница'],index=False,float_format='%.2f')
    writer.save()
    print('Файл с отличиями сформирован')
if __name__ == '__main__':
    try:
        data = get_json()  #Выгрузка из CRM-застройщика в формате JSON только по свободным квартирам в DataFrame
        data = maintain_df(data)  # обрабатываем DataFrame (выбираем только Облака, преобразуем данные в float и отсеиваем лишние колонки)
        data = merge_df(data)  # прводим "левое" слияние с выгрузкой Васильева, т.к. данные Васильева являются приоритетными. Имея данные Васильева можно не считывать JSON, но вдруг у него что-то пропадет
        writer = pd.ExcelWriter('Summary 2019-01-16.xlsx')
        data.to_excel(writer,
                      columns=['Код объекта', 'Условный номер', 'Статус', 'Площадь', 'Цена', 'Отделка_y', 'Дата договора'],
                      index=False) #Записываем данные для будущих сравнений
        writer.save()
        compare_df(data)
        print('Всё готово!')
        input('Для продолжения нажми Enter')
    except SyntaxError:
        pass
    except PermissionError:
        print('Ошибка! Закрой открытые файлы!')
        input('Для продолжения нажми Enter')
        pass
    except LookupError:
        print('Ошибка! Что-то не так с названиями ключевых колонок')
        input('Для продолжения нажми Enter')
        pass

