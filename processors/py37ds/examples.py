import pandas as pd

from src.config import metadata_engine
from src.readers import Reader, Writer


def main():
    app_name = pd.read_sql_table("app_info", metadata_engine)["app_name"].iat[0]
    processor_backend_name = pd.read_sql_table("processor_backend_info", metadata_engine)["processor_backend_name"].iat[0]

    reader = Reader(app_name, processor_backend_name)
    writer = Writer(app_name, processor_backend_name)

    # first script
    visits_workorders = reader.read_by_draft_name("visits_workorders_view")
    normalized_visits = visits_workorders.copy(deep=True)

    writer.write_by_draft_name_single_sink(normalized_visits, "normalized_visits")

    # second script
    transactions = reader.read_by_draft_name("transactions_view")
    reversed_transactions = transactions.copy(deep=True)

    writer.write_by_draft_name_single_sink(reversed_transactions, "reversed_transactions")

    # third script
    normalized_visits = reader.read_by_draft_name("normalized_visits")
    reversed_transactions = reader.read_by_draft_name("reversed_transactions")
    bound_transactions_ambient_visits = reversed_transactions.copy(deep=True)

    writer.write_by_draft_name_single_sink(bound_transactions_ambient_visits, "bound_transactions_ambient_visits")

    # fourth script
    normalized_visits = reader.read_by_draft_name("normalized_visits")
    reversed_transactions = reader.read_by_draft_name("reversed_transactions")
    bound_transactions_ambient_visits = reader.read_by_draft_name("bound_transactions_ambient_visits")
    features_for_PN = bound_transactions_ambient_visits.copy(deep=True)

    writer.write_by_draft_name_single_sink(features_for_PN, "features_for_pn")

    # prediction
    abstract_model_name = "model_name"
    # создаём modelReader
    # важно: здесь нас не интересует, на каком model backend натренирована модель
    model_reader = ModelReader(processor_backend_name)
    # получаем model_implementation
    # сначала implementationDiscovery
    # затем считываем модель по model_implementation_info с помощью ModelDriver
    model_implementation = model_reader.read_by_model_name(abstract_model_name)
    # предсказываем нечто с помощью модели и записываем результаты в таблицы
    prediction = model_implementation.predict_by_app_name(app_name)


if __name__ == "__main__":
    main()



