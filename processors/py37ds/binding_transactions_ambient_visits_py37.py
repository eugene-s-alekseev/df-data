def binding_transactions_ambient_visits_py37():
    import re

    import numpy as np
    from tqdm import tqdm

    from src.readers import Reader, Writer

    app_name = "test_app"
    processor_backend_name = "py37ds"

    """
    Данный скрипт ищет визит, для которого была выдача на борт. 
    """
    DAYS_BEFORE_VISIT = 7
    DAYS_AFTER_VISIT = 3

    #### Словарь описания партномеров
    ### забираем все очищенные транзакции , необходимые для текущих взитов
    reader = Reader(app_name, processor_backend_name)
    df_PN = reader.read_by_draft_name("reversed_transactions")
    df_PN = df_PN[['ITEM_NAME', 'ITEM_DESCRIPTION', 'REAL_TRANSACTION_QUANTITY_FLOAT_NEW']].copy()

    df_PN = df_PN.groupby(['ITEM_NAME', 'ITEM_DESCRIPTION']).agg(
        {'REAL_TRANSACTION_QUANTITY_FLOAT_NEW': 'count'}).reset_index()

    df_PN.sort_values('REAL_TRANSACTION_QUANTITY_FLOAT_NEW', ascending=False, inplace=True)
    """ почистили лишне шапки """
    df_PN.drop_duplicates(subset='ITEM_NAME', inplace=True)

    """ словарь всех партномеров , коотрые есть в выдаче  (зачем от тут ????? )"""
    dict_item_de = dict(zip(df_PN.ITEM_NAME, df_PN.ITEM_DESCRIPTION))

    """ забираем сами транзакции """

    df_PN = reader.read_by_draft_name("reversed_transactions")
    df_PN = df_PN[df_PN.REAL_TRANSACTION_QUANTITY_FLOAT_NEW > 0].copy()
    df_PN = df_PN[df_PN.STATYA_TMC.isin({'АТИ.Материалы на ТО',
                                         'АТИ.АиРЭО.СД',
                                         'Планеры.ВСУ.Двигатели'})].copy()
    df_PN = df_PN[['TRANSACTION_DATE',
                   'SOB_PLANE',
                   'ITEM_NAME',
                   'UOM_NAME',
                   'REAL_TRANSACTION_QUANTITY_FLOAT_NEW']].copy()
    """ выкидываем все транзакции, где нет самолета """
    df_PN.dropna(subset=['SOB_PLANE'], inplace=True)

    """ словарь самолетов """
    df_AC = reader.read_by_draft_name("data_ac_for_ws")
    df_AC['SOB_PLANE'] = df_AC.EXTERNAL_REFERENCE.apply(lambda x: re.sub('[^\w]', '', x))
    dict_AC = dict(zip(df_AC['SOB_PLANE'], df_AC['INSTANCE_ID']))

    """ для всех транзакциях выдачи получаем интанс ид самолета """
    df_PN['INSTANCE_ID'] = df_PN.SOB_PLANE.map(dict_AC)

    df_PN.dropna(subset=['INSTANCE_ID'], inplace=True)

    df_PN['TRANSACTION_DATE'] = df_PN.TRANSACTION_DATE.apply(lambda x: np.datetime64(str(x)))
    """ нормализовали все описания  партномеров """
    df_PN['ITEM_DESCRIPTION'] = df_PN.ITEM_NAME.map(dict_item_de)

    ### забираем все визиты , как есть (не обработанные )
    df_visits_all = reader.read_by_draft_name("visits_workorders_view")
    df_visits_all.dropna(inplace=True)

    df_visits_all['START_DATE_TIME'] = df_visits_all.START_DATE_TIME.apply(lambda x: np.datetime64(str(x))
                                                                            if str(x) != 'nan'
                                                                            else np.nan)

    df_visits_all['CLOSE_DATE_TIME'] = df_visits_all.CLOSE_DATE_TIME.apply(lambda x: np.datetime64(str(x))
                                                                            if str(x) != 'nan'
                                                                            else np.nan)

    df_visits_all['days_in_WS'] = round(
        (df_visits_all.CLOSE_DATE_TIME - df_visits_all.START_DATE_TIME) / np.timedelta64(1, 'D'))

    df_visits_all['ACTUAL_START_DATE'] = df_visits_all.ACTUAL_START_DATE.apply(lambda x: np.datetime64(str(x))
                                                                                if str(x) != 'nan'
                                                                                else np.nan)

    df_visits_all['ACTUAL_END_DATE'] = df_visits_all.ACTUAL_END_DATE.apply(lambda x: np.datetime64(str(x))
                                                                            if str(x) != 'nan'
                                                                            else np.nan)

    df_visits_all['ACTUAL_days_in_WS'] = round(
        (df_visits_all.ACTUAL_END_DATE - df_visits_all.ACTUAL_START_DATE) / np.timedelta64(1, 'D'))
    df_visits_all.sort_values('ACTUAL_days_in_WS', ascending=False, inplace=True)

    df_visits_dict = reader.read_by_draft_name("normalized_visits")
    long_visits = list(df_visits_dict.EMBRACING_VISIT_ID.unique())

    """ костыльное решение """
    """ тут для всех долгих визитов ищутся все транзакции , которые им  подходят по датам и по самолету """

    df_PN['VISIT_ID'] = None

    for visit in tqdm(long_visits):
        try:
            inner_visits = set(df_visits_dict[df_visits_dict.EMBRACING_VISIT_ID == visit].VISIT_ID.values) | {visit}
            AC = df_visits_all[df_visits_all.VISIT_ID.isin(inner_visits)].INSTANCE_ID.values[0]
            min_date = df_visits_all[df_visits_all.VISIT_ID.isin(inner_visits)].ACTUAL_START_DATE.min()
            max_date = df_visits_all[df_visits_all.VISIT_ID.isin(inner_visits)].ACTUAL_END_DATE.max()

            df_PN.at[df_PN[(df_PN.INSTANCE_ID == AC) &
                       (df_PN.TRANSACTION_DATE >= (min_date - np.timedelta64(DAYS_BEFORE_VISIT, 'D'))) &
                       (df_PN.TRANSACTION_DATE <= (
                                   max_date + np.timedelta64(DAYS_AFTER_VISIT, 'D')))].index, 'VISIT_ID'] = visit
        except:
            continue

    df_PN.dropna(inplace=True)

    """ записываем результат """
    writer = Writer(app_name, processor_backend_name)
    writer.write_by_draft_name_single_sink(df_PN, "bound_transactions_ambient_visits")
