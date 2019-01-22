import pandas as pd

grishin_price = pd.read_excel('grishin_price.xlsx', usecols=[0,8,10,9])
grishin_price = grishin_price.rename(
        columns={'Стоимость продажи': 'Grishin_price','Отделка': 'Grishin_decoration', 'Вывод в продажу 1/0': 'Grishin_status'})
grishin_price = grishin_price.replace({'б/о': 0, 'черновая': 1, 'чистовая': 2, 'Чистовая МП':2, 'чистовая старая': 2})
oblaka_price = pd.read_excel('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\Облака прайс.xlsx')
check = pd.merge(oblaka_price,grishin_price, how='inner', on='Код объекта')
for i in range (len(check)):
    if (pd.notnull(check.loc[i,'Цена'])):
        check.loc[i,'Price_differ'] = round(check.loc[i,'Grishin_price'] - check.loc[i,'Цена'],0)
    check.loc[i,'Status_differ'] = check.loc[i,'Grishin_status'] - check.loc[i,'Доступность к продаже']
    check.loc[i, 'Decoration_differ'] = check.loc[i, 'Grishin_decoration'] - check.loc[i, 'Отделка_y']
check = check.drop(columns=['Дата договора'])
check = check[check['Price_differ'].notnull()]
check = check[(abs(check['Price_differ'])>4) | (abs(check['Status_differ'])>0) | (abs(check['Decoration_differ'])>0)]
check.to_csv('1.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
writer = pd.ExcelWriter('\\\\192.168.10.123\\it\\Иван\\ИВАН\\БСА-ДОМ исходники\\exp\\Сверка Облаков.xlsx')
check.to_excel(writer, '1', columns=['Код объекта','Секция','Стояк','Условный номер','Площадь','Комнат','Доступность к продаже','Цена','Цена за метр','Отделка_y','Grishin_price',
                                     'Price_differ','Grishin_status','Status_differ','Decoration_differ'],index=False)
writer.save()

