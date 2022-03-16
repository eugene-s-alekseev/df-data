def get_features_for_pn_py37():
    import re
    import warnings

    import numpy as np
    from tqdm import tqdm

    from src.readers import Writer, Reader

    warnings.filterwarnings("ignore")

    app_name = "test_app"
    processor_backend_name = "py37ds"

    reader = Reader(app_name, processor_backend_name)

    """
    Этот скрипт формирует признаки и таргеты из выдачи на борт. 

    Фичи  по PN : 
    -   последняя выдача , сколько дней назад;
    -   кол-во выдачи в последний раз;  
    -   сумма по всем выдачам; 

    Таргеты по PN:  
    - выдача данного PN на данный визит;   

    """

    df_visits = reader.read_by_draft_name("normalized_visits")

    df_visits = df_visits[['VISIT_ID', 'INSTANCE_ID', 'ACTUAL_START_DATE']].drop_duplicates().copy()

    df_visits['ACTUAL_START_DATE'] = df_visits.ACTUAL_START_DATE.apply(lambda x: np.datetime64(x))

    """ загружаем результаты 1_get..... """
    df_PN = reader.read_by_draft_name("pn_purchase_visit_id_dict")
    df_PN['TRANSACTION_DATE'] = df_PN.TRANSACTION_DATE.apply(lambda x: np.datetime64(x))

    df_PN_dict = reader.read_by_draft_name("pn_dict")

    def get_days_before(AC, date, VISIT_ID, df_find):
        """
        Выдает:
        -выдачу в это визит;
        -прошлую выдачу этого PN;
        -кол-во дней с прошлой выдачи этого PN;
        -обратную сумму всех прошлых выдачей;

        """
        this_visit_qti = df_find[df_find.VISIT_ID == VISIT_ID]["REAL_TRANSACTION_QUANTITY_FLOAT_NEW"].sum()

        # Учтем , что могли выдать заранее
        df_temp = df_find[(df_find.INSTANCE_ID == AC) &
                          (df_find.TRANSACTION_DATE < (date - np.timedelta64(7, 'D')))].copy()

        if len(df_temp) > 0:
            df_temp['delta'] = abs(df_temp['TRANSACTION_DATE'] - date) / np.timedelta64(1, 'D')
            df_temp.sort_values('delta', inplace=True)

            last_qti = df_temp["REAL_TRANSACTION_QUANTITY_FLOAT_NEW"].values[0]
            min_days_ago = round(df_temp['delta'].min(), 1)

            # сумма по всем выдачам
            df_temp['delta'] = df_temp['REAL_TRANSACTION_QUANTITY_FLOAT_NEW'] / df_temp['delta']
            return this_visit_qti, last_qti, min_days_ago, round(df_temp['delta'].sum(), 4)
        return this_visit_qti, None, None, None

    writer = Writer(app_name, processor_backend_name)

    for item_name, item_desc, uom, name_for_file in tqdm(df_PN_dict[:2].values):
        if re.sub('[^A-Z0-9]', '', name_for_file) == '':
            continue

        df_PN_temp = df_PN[(df_PN.ITEM_NAME == item_name) &
                           (df_PN.ITEM_DESCRIPTION == item_desc) &
                           (df_PN.UOM_NAME == uom)].copy()

        df_visits['temp'] = df_visits.apply(lambda row:
                                            get_days_before(row['INSTANCE_ID'],
                                                            row['ACTUAL_START_DATE'],
                                                            row['VISIT_ID'],
                                                            df_find=df_PN_temp), axis=1)

        df_visits['TARGET'] = df_visits.temp.apply(lambda x: x[0])
        df_visits[name_for_file + '_last_qti'] = df_visits.temp.apply(lambda x: x[1])
        df_visits[name_for_file + '_days_ago'] = df_visits.temp.apply(lambda x: x[2])
        df_visits[name_for_file + '_sum_qti_vs_days'] = df_visits.temp.apply(lambda x: x[3])
        df_visits.drop('temp', axis=1, inplace=True)
        writer.write_by_draft_name_single_sink(df_visits.drop('ACTUAL_START_DATE', axis=1), name_for_file.upper())
        df_visits = df_visits[['VISIT_ID', 'INSTANCE_ID', 'ACTUAL_START_DATE']].copy()
