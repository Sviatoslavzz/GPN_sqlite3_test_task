import pandas as pd
import numpy as np
import datetime as dt
import sqlite3 as sq
import os


files_path_list = []
# скрипт должен сам найти директорию, где выполняется и проверить файлы в папке 'sources'
for file in os.listdir(os.path.dirname(os.path.abspath(__file__)) + '/sources'):
    if file.endswith('.xlsx'):
        files_path_list.append(os.getcwd() + '/sources' + '/' + file)

for file in files_path_list:
    df = pd.read_excel(file, skiprows=3, usecols="A:M", header=None)

    # создание корректных заголовков
    # таблица "ПФ на год"
    df.rename(columns={
        0: df[0][0],
        1: df[1][0],
        2: df[2][0],
        3: df[3][0],
        4: df[4][0],
        5: df[5][1],
        6: df[6][1],
        7: df[7][2],
        8: df[8][1],
        9: df[9][2],
        10: df[10][2],
        11: df[11][2],
        12: df[12][2]
    }, inplace=True)

    # удаление лишних строчек между заголовками и данными
    df = df.drop(labels=range(0, 6), axis=0)

    # замена символов переноса строки в заголовках на пробел
    new_title = ''
    for col in enumerate(df.columns):
        if '\n' in col[1]:
            new_title = col[1].replace('\n', ' ')
        else:
            new_title = col[1]
            df.rename(columns={
              df.columns[col[0]]: new_title
            }, inplace=True)

    # удаление лишних строчек после таблицы с данными
    # для этого смотрим последние строки по первому столбцу
    df = df.drop(index=df[df["Наименование инвестиционного проекта"].isna()].index, axis=0)
    df = df.reset_index(drop=True)

    # собираем дф по каждому месяцу циклом
    month = [
        'january',
        'february',
        'march',
        'april',
        'may',
        'june',
        'july',
        'august',
        'september',
        'october',
        'november',
        'december'
    ]
    cols_range = [
        'A:C, W:AE',
        'A:C, AF:AN',
        'A:C, AO:AW',
        'A:C, BI:BQ',
        'A:C, BR:BZ',
        'A:C, CA:CI',
        'A:C, CU:DC',
        'A:C, DD:DL',
        'A:C, DM:DU',
        'A:C, EG:EO',
        'A:C, EP:EX',
        'A:C, EY:FG'
    ]

    data_full = []

    for i in range(12):
        # собираем дф по каждому месяцу
        df_month = pd.read_excel(file, skiprows=3, usecols=cols_range[i], header=None)

        # для удобства пронумеруем колонки
        for j, col in enumerate(df_month.columns):
            if j >= 3:
                df_month.rename(columns={
                    col: j
                }, inplace=True)

        # создание корректных заголовков для таблиц по месяцам
        df_month.rename(columns={
            0: df_month[0][0],
            1: df_month[1][0],
            2: df_month[2][0],
            3: df_month[3][3],
            4: df_month[4][3],
            5: df_month[5][4].replace('\xa0', ' '),
            6: df_month[6][3],
            7: df_month[7][3],
            8: df_month[8][4],
            9: df_month[9][4],
            10: df_month[10][4],
            11: df_month[11][4].replace('\n', '')
        }, inplace=True)

        # удаление лишних строчек между заголовками и данными
        df_month = df_month.drop(labels=range(0, 6), axis=0)

        # удаление лишних строчек после таблицы с данными
        # для этого смотрим последние строки по первому столбцу
        df_month = df_month.drop(index=df_month[df_month["Наименование инвестиционного проекта"].isna()].index, axis=0)
        df_month = df_month.reset_index(drop=True)

        # запишем таблицу по месяцу в список
        data_full.append(df_month)

        # удалим отработанную переменную
        del df_month

    # добавляем основной дф в список к дф по месяцам по индексу 0
    data_full.insert(0, df)
    del df

    # корректируем заголовки у всех таблиц
    for df in data_full:
        for column in df.columns:
            df.rename(columns={
                column: '_'.join(i for i in column.split(' ') if i != '')
            }, inplace=True)

    # корректировка типов признаков по каждому дф
    for i, col in enumerate(data_full[0].columns):
        # игнорируем 3 первых столбца
        if i > 2:
            # преобразование всех числовых значений в числа, а ошибки в NaN
            data_full[0][col] = pd.to_numeric(data_full[0][col], errors='coerce')
            # преобразуем все числовые данные к типу float
            data_full[0][col] = data_full[0][col].astype({
                col: float
            }, errors='ignore').round(2)

    # корректируем типы признаков для таблиц по месяцам
    for i in range(1, 13):
        for j, col in enumerate(data_full[i].columns):
            if j > 2:
                data_full[i][col] = pd.to_numeric(data_full[i][col], errors='coerce')
        for j, col in enumerate(data_full[i]):
            if j > 2:
                data_full[i] = data_full[i].astype({
                  col: float
                }, errors='ignore').round(2)

    # Работа с sqlite3
    db_name = file.split('/')
    with sq.connect(os.getcwd() + '/databases' + '/' + db_name[-1].replace('.xlsx', '.db')) as con:
        cur = con.cursor()
        db_names = [
          'ПФ на год',
          'январь',
          'февраль',
          'март',
          'апрель',
          'май',
          'июнь',
          'июль',
          'август',
          'сентябрь',
          'октябрь',
          'ноябрь',
          'декабрь',
        ]

        for i in range(13):
            data_full[i].to_sql(name=db_names[i], con=con, index=False, if_exists='replace')

        con.commit()