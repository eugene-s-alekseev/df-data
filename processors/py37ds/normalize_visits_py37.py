def normalize_visits_py37():
    import numpy as np

    from src.readers import Reader, Writer

    app_name = "test_app"
    processor_backend_name = "py37ds"

    reader = Reader(app_name, processor_backend_name)
    df = reader.read_by_draft_name("visits_workorders_view")

    # далее идут оригинальный код и комменты Любы
    MIN_length_for_visit = 1
    df.dropna(inplace=True)

    # приводим к формату дата-время
    df['START_DATE_TIME'] = df.START_DATE_TIME.apply(lambda x: np.datetime64(str(x)[0:10])
                                                               if str(x) != 'nan'
                                                               else np.nan)

    df['CLOSE_DATE_TIME'] = df.CLOSE_DATE_TIME.apply(lambda x: np.datetime64(str(x)[0:10])
                                                               if str(x) != 'nan'
                                                               else np.nan)

    df['days_in_WS'] = round((df.CLOSE_DATE_TIME - df.START_DATE_TIME) / np.timedelta64(1, 'D'))

    # приводим к формату дата-время
    df['ACTUAL_START_DATE'] = df.ACTUAL_START_DATE.apply(lambda x: np.datetime64(str(x)[0:10])
                                                                   if str(x) != 'nan'
                                                                   else np.nan)

    df['ACTUAL_END_DATE'] = df.ACTUAL_END_DATE.apply(lambda x: np.datetime64(str(x)[0:10])
                                                               if str(x) != 'nan'
                                                               else np.nan)

    df['ACTUAL_days_in_WS'] = round((df.ACTUAL_END_DATE - df.ACTUAL_START_DATE) / np.timedelta64(1, 'D'))
    df.sort_values('ACTUAL_days_in_WS', ascending=False, inplace=True)

    # Данная сортировка нужна обязательно , что бы выбирать приоритетный объемлющий визит !
    # выявляем медианную длинну визита для типа визита
    df_long_visits = df[df.ACTUAL_days_in_WS > MIN_length_for_visit].copy()

    # Данная сортировка нужна обязательно , что бы выбирать приоритетный объемлющий визит !

    # выявляем медианную длинну визита для типа визита"""
    df_VISIT_TYPE_CODE_dict = df_long_visits.groupby('VISIT_TYPE_CODE').agg(
        {'ACTUAL_days_in_WS': 'median'}).reset_index()
    df_VISIT_TYPE_CODE_dict.sort_values(['ACTUAL_days_in_WS', 'VISIT_TYPE_CODE'], ascending=False, inplace=True)

    # print('Словарь приоритета по типам визита')"""
    # print(df_VISIT_TYPE_CODE_dict)"""

    df_VISIT_TYPE_CODE_dict = dict(zip(df_VISIT_TYPE_CODE_dict.VISIT_TYPE_CODE,
                                       df_VISIT_TYPE_CODE_dict.ACTUAL_days_in_WS))
    # по этому полю будет сортировка для определения более приоритетного визита по типу """
    df_long_visits['for_sorting'] = df_long_visits.VISIT_TYPE_CODE.map(df_VISIT_TYPE_CODE_dict)

    # Данная сортировка нужна обязательно , что бы выбирать приоритетный объемлющий визит !
    df_long_visits.sort_values(['ACTUAL_days_in_WS', 'for_sorting'], ascending=False, inplace=True)

    def get_biggest_WS_for_cur_WS(df_for_find,
                                  AC,
                                  date_beg,
                                  date_end):
        """
        df_for_find-тут ищем объемлющий визит
        WS_Lenth - длинна текущего визита (объемлющий визит м.б. такой же или больше в днях)
        Для визита пытаемся найти пересекающийся более длинный визит.
        Или такой же длинны, но с приоритетным типом визитом.
        Если не находим, то нет объемлющего визита.
        """
        df_temp = df_for_find[(df_for_find.INSTANCE_ID == AC) &
                              (((df_for_find.ACTUAL_START_DATE <= date_beg) &
                                (df_for_find.ACTUAL_END_DATE >= date_beg)) |
                               ((df_for_find.ACTUAL_START_DATE <= date_end) &
                                (df_for_find.ACTUAL_END_DATE >= date_end)))].copy()
        if len(df_temp) > 0:
            return (df_temp['VISIT_ID'].values[0])
        else:
            return None

    ### В первую очередь проверим вложенность среди длинных визитов
    df_long_visits['max_VISIT_ID'] = df_long_visits.apply(lambda row: get_biggest_WS_for_cur_WS(
                                                                        df_for_find=df_long_visits,
                                                                        AC=row['INSTANCE_ID'],
                                                                        date_beg=row['ACTUAL_START_DATE'],
                                                                        date_end=row['ACTUAL_END_DATE']), axis=1)

    df['max_VISIT_ID'] = df.apply(lambda row: get_biggest_WS_for_cur_WS(
                                                df_for_find=df_long_visits,
                                                AC=row['INSTANCE_ID'],
                                                date_beg=row['ACTUAL_START_DATE'],
                                                date_end=row['ACTUAL_END_DATE']), axis=1)

    #Устранение вложенности длинных визитов друг в друга
    dict_1=dict(zip(df_long_visits['VISIT_ID'],df_long_visits['max_VISIT_ID']))
    df['embracing_VISIT_ID']=df.max_VISIT_ID.map(dict_1)

    writer = Writer(app_name, processor_backend_name)
    writer.write_by_draft_name_single_sink(df, "normalized_visits")
