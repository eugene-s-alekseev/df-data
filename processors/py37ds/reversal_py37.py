def reversal_py37():
    import datetime
    import warnings
    warnings.filterwarnings('ignore')

    from tqdm import tqdm

    from src.readers import Reader, Writer

    app_name = "test_app"
    processor_backend_name = "py37ds"

    reader = Reader(app_name, processor_backend_name)
    df = reader.read_by_draft_name("transactions_view")

    # далее - оригинальный код и комменты Любы

    # """дропаем заголовки """
    df = df[df['ORGANIZATION_ID'] != 'ORGANIZATION_ID'].copy()
    # убираем задвоение в данных
    df.drop_duplicates(inplace=True)
    # индексация
    df['index'] = df.index
    # """приводим дату в тип даты """
    df['TRANSACTION_DATE'] = df.TRANSACTION_DATE.apply(lambda x: datetime.datetime.strptime(str(x)[0:10], "%Y-%m-%d"))
    # """сортируем по дате , самые ранние -наверх"""
    df.sort_values('TRANSACTION_DATE', inplace=True)
    # """создаем новый столбец с кол-вом транзакций """
    df['TRANSACTION_QUANTITY_float'] = df.TRANSACTION_QUANTITY.apply(lambda x: float(x))
    # """создаем новый столбец с кол-вом транзакций, его значения потом будем менять (если вернули не все, а только часть) """

    df['REAL_TRANSACTION_QUANTITY_float'] = df.REAL_TRANSACTION_QUANTITY.apply(lambda x: float(x))

    def stornation_of_minus_transaction(df_temp, df, TRANSACTION_QUANTITY):
        """
        Функция находит по мунусовой транзакции
        транзакцию для сторнирования.
        И помечает ее для удаления.
        df_temp- все данные (кусок общего датафрейма) котрые нужны для сторнирования
        """
        # """это сумма минусовой транзакции, которую мы хотим сторнировать """

        if len(df_temp) > 0:
            # """ транзакции , где значение кол-ва в точности совпадает (такие сторнируем в первую очередь )"""
            df_temp_1 = df_temp[(df.REAL_TRANSACTION_QUANTITY_float == abs(TRANSACTION_QUANTITY))].copy()
            if len(df_temp_1) > 0:
                ind_for_edit = df_temp['index'].values[0]
                # """помечаем найденные транзакции для удаления (в исходном датасет)"""
                df.at[index, 'for_drop'] = True
                df.at[ind_for_edit, 'for_drop'] = True

    ##### Нужно убрать сторнированные транзакции (то, что вернули с ВС)
    ####Поле, которое нужно будет менять
    df['REAL_TRANSACTION_QUANTITY_float_new'] = df['REAL_TRANSACTION_QUANTITY_float']
    # """поле с флагом , которое будем менять """
    df['for_drop'] = False
    # """минусовые транзакции , которые мы должны выкинуть """
    df_minus = df[df.REAL_TRANSACTION_QUANTITY_float < 0].copy()
    # """ выкидываем все транзакции , которые не привязаны к самолету  """
    df_minus.dropna(subset=['SOB_PLANE'], inplace=True)
    for index, \
        PN_name, \
        date, \
        TRANSACTION_QUANTITY, \
        SOB_PLANE in tqdm(df_minus[['index',
                                    'ITEM_NAME',
                                    'TRANSACTION_DATE',
                                    'REAL_TRANSACTION_QUANTITY_float',
                                    'SOB_PLANE']].values):

        df_temp = df[
            # Расширила период поиска транзакции, на всякий случай (если склад закрывали за квартал, например)
            (df["TRANSACTION_DATE"] <= date) &
            (df["TRANSACTION_DATE"] >= date - datetime.timedelta(90)) &
            (df.SOB_PLANE == SOB_PLANE) &
            (df.ITEM_NAME == PN_name) &
            # (df.REAL_TRANSACTION_QUANTITY_float>=abs(TRANSACTION_QUANTITY))&
            (df.REAL_TRANSACTION_QUANTITY_float >= 0) &
            (df.for_drop == False)].copy()

        df_temp.sort_values('TRANSACTION_DATE', ascending=False, inplace=True)
        try:
            # """Тут транзакции либо схлопнуться либо в положительной изменится значение"""
            stornation_of_minus_transaction(df_temp, df, TRANSACTION_QUANTITY)
        except:
            print('ERROR')

    writer = Writer(app_name, processor_backend_name)
    writer.write_by_draft_name_single_sink(df, "reversed_transactions")
