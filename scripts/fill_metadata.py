import datetime as dt
from hashlib import md5
from json import dumps

from networkx import DiGraph

from src.build_dag import DagBuilder
from src.config import sql_source_connection_string, postgres_pdm_localhost_connection_string, \
    postgres_pdm_connection_string, path_to_sources
from src.data_model import AbstractProcessorInfoFactory, AppInfoFactory, SourceInfoFactory, DraftInfoFactory, ProcessorBackendInfoFactory, \
    ProcessorImplementationInfoFactory, DatasetImplementationInfoFactory, DriverInfoFactory, \
    ProcessorUponProcessorDependencyInfoFactory, DagFlowLevelInfoFactory, \
    AbstractProcessorToImplementationMappingInfoFactory, DagImplementationInfoFactory, DagBuildInfoFactory
from src.readers import Reader
from src.registration import ProcessorImplementationHandler, DatasetImplementationHandlerFactory, DriverHandler, DagBuildHandler, \
    DagFlowLevelHandler, AbstractProcessorToImplementationMappingHandler, DagImplementationHandler


def main():
    APP_NAME = "test_app"
    PROCESSOR_BACKEND_NAME = "py37ds"
    DAG_FLOW_LEVEL_NAME = "test_dag_flow_level"

    # сначала роняем тот граф, который у нас есть в neo4j
    DagFlowLevelHandler().drop_graph_by_flow_name(DAG_FLOW_LEVEL_NAME)

    # Abstract Processor - это абстракция, которая соответствует какому-то кусочку пайплайна. Он характеризуется
    # первичными источниками (drafts), из которых считывает данные, а также sinks (draft), в которые эти данные записывает.
    # Важно понимать, что abstract processors, primary sources и sinks - это абстракции, не привязанные к физической
    # реализации.
    # Чтобы в дальнейшем работать с конкретными реализациями, сначала нужно зарегистрировать abstract processors.
    # Важно: это одна из абстракций, которые тебе придётся использовать наиболее часто.
    normalized_visits_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("normalize_visits")
    reversal_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("reversal")
    binding_transactions_ambient_visits_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("binding_transactions_ambient_visits")
    get_features_for_pn_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("get_features_for_pn")

    # App - это абстракция, которая, грубо говоря, соотвествует набору первичных источников.
    # Например, если у нас есть потоки данных, для которых нужно каждый день делать какие-то предсказания, и пакет данных
    # с той же схемой, для которого предсказание нужно сделать однократно, то для этих двух наборов данных нужно
    # зарегистрировать два разных приложения.
    # Apps необходимы для того, чтобы регистрировать dataset implementations и dag builds.
    test_app_info = AppInfoFactory.construct_by_name(APP_NAME)

    # Source - это некоторое хранилище данных, например Postgres, sqlite, папка с csv и так далее.
    # Чтобы пользоваться источниками (читать из них данные и записывать их), нужно зарегистрировать хотя бы
    # один драйвер.
    sqlite_connection_info = dumps(
        dict(connection_string=sql_source_connection_string))
    sqlite_source_info = SourceInfoFactory.construct_by_name("sqlite3-public", sqlite_connection_info)

    postgres_pdm_connection_info = dumps(dict(connection_string=postgres_pdm_connection_string))
    postgres_pdm_source_info = SourceInfoFactory.construct_by_name("postgres-pdm",
                                                                   postgres_pdm_connection_info)

    postgres_pdm_localhost_connection_info = dumps(
        dict(connection_string=postgres_pdm_localhost_connection_string))
    postgres_pdm_localhost_source_info = SourceInfoFactory.construct_by_name("postgres-pdm-localhost",
                                                                             postgres_pdm_localhost_connection_info)

    csv_metadata_connection_info = dumps(dict(path_to_data=f"{path_to_sources}/csv_metadata"))
    csv_metadata_source_info = SourceInfoFactory.construct_by_name("csv_metadata",
                                                                   csv_metadata_connection_info)

    # Drafts - это логическое описание данных, не привязанное к конкретному источнику. Drafts используются разработчиками
    # процессоров (processors), чтобы абстрагироваться от конкретных хранилищ данных.
    # Чтобы работать с данными и регистрировать их конкретные реализации, сначала нужно зарегистрировать drafts.
    # Важно: сейчас фреймворк не предполагает возможность контроля схемы.
    # Важнo: эта одна из абстракций, которые тебе придётся регистрировать руками наиболее часто.
    visits_workorders_view_draft_info = DraftInfoFactory.construct_by_name("visits_workorders_view")
    normalized_visits_draft_info = DraftInfoFactory.construct_by_name("normalized_visits")
    transactions_view_draft_info = DraftInfoFactory.construct_by_name("transactions_view")
    reversed_transactions_draft_info = DraftInfoFactory.construct_by_name("reversed_transactions")
    bound_transactions_ambient_visits_draft_info = DraftInfoFactory.construct_by_name("bound_transactions_ambient_visits")
    pn_dict_draft_info = DraftInfoFactory.construct_by_name("pn_dict")
    data_ac_for_ws_draft_info = DraftInfoFactory.construct_by_name("data_ac_for_ws")
    pn_purchase_visit_id_dict_draft_info = DraftInfoFactory.construct_by_name("pn_purchase_visit_id_dict")

    # Processor backend - бакенд, на котором разработчики пишут процессоры. Необходим для того, чтобы находить
    # подходящие драйверы, а также собирать таски в airflow.
    py37ds_processor_backend_info = ProcessorBackendInfoFactory.construct_by_name(PROCESSOR_BACKEND_NAME)

    # Processor implementation - реализация абстрактного процессора на конкретном бэкенде.
    # Чтобы использовать в графах реализации процессоров, их нужно сначала зарегистрировать
    # Важно: это одна из абстракций, которые тебе придётся использовать наиболее часто.
    normalized_visits_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "normalize_visits_py37",
        normalized_visits_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(normalized_visits_processor_implementation_info)

    reversal_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "reversal_py37",
        reversal_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(reversal_processor_implementation_info)

    binding_transactions_ambient_visits_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "binding_transactions_ambient_visits_py37",
        binding_transactions_ambient_visits_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(binding_transactions_ambient_visits_processor_implementation_info)

    get_features_for_pn_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "get_features_for_pn_py37",
        get_features_for_pn_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(get_features_for_pn_processor_implementation_info)

    # Dataset implementation - это реализация draft в конкретном хранилище данных.
    # Обычно разработчик процессора не использует dataset implementation напрямую, а пользуется вместо этого
    # draft_id или draft_name.
    # Чтобы испольвать dataset implementation, его сначала нужно зарегистрировать; у каждого draft может быть
    # несколько dataset implementations, для каждого из которых установлен приоритет на чтение и запись.
    # Важно: это одна из абстракций, которые тебе придётся использовать наиболее часто.

    # normalized_visits_sqlite = DatasetImplementationInfoFactory.construct_by_name(
    #     "normalized_visits_sqlite",
    #     test_app_info,
    #     normalized_visits_draft_info,
    #     sqlite_source_info
    # )
    # DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(normalized_visits_sqlite)
    # DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(normalized_visits_sqlite)
    #
    # reversed_transactions_sqlite = DatasetImplementationInfoFactory.construct_by_name(
    #     "reversed_transactions_sqlite",
    #     test_app_info,
    #     reversed_transactions_draft_info,
    #     sqlite_source_info
    # )
    # DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(reversed_transactions_sqlite)
    # DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(reversed_transactions_sqlite)
    #
    # bound_transactions_ambient_visits_sqlite = DatasetImplementationInfoFactory.construct_by_name(
    #     "bound_transactions_ambient_visits_sqlite",
    #     test_app_info,
    #     bound_transactions_ambient_visits_draft_info,
    #     sqlite_source_info
    # )
    # DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
    #     bound_transactions_ambient_visits_sqlite)
    # DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(
    #     bound_transactions_ambient_visits_sqlite)

    visits_workorders_view_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
        "visits_workorders_view_postgres_pdm_localhost",
        test_app_info,
        visits_workorders_view_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        visits_workorders_view_postgres_pdm_localhost)

    normalized_visits_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
        "normalized_visits_postgres_pdm_localhost",
        test_app_info,
        normalized_visits_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        normalized_visits_postgres_pdm_localhost)
    DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(
        normalized_visits_postgres_pdm_localhost)

    transactions_view_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
        "transactions_view_postgres_pdm_localhost",
        test_app_info,
        transactions_view_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        transactions_view_postgres_pdm_localhost)

    reversed_transactions_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
        "reversed_transactions_postgres_pdm_localhost",
        test_app_info,
        reversed_transactions_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        reversed_transactions_postgres_pdm_localhost)
    DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(
        reversed_transactions_postgres_pdm_localhost)

    bound_transactions_ambient_visits_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
        "bound_transactions_ambient_visits_postgres_pdm_localhost",
        test_app_info,
        bound_transactions_ambient_visits_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        bound_transactions_ambient_visits_postgres_pdm_localhost)
    DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(
        bound_transactions_ambient_visits_postgres_pdm_localhost)

    data_ac_for_ws_postgres = DatasetImplementationInfoFactory.construct_by_name(
        "data_ac_for_ws_postgres",
        test_app_info,
        data_ac_for_ws_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(data_ac_for_ws_postgres)

    pn_dict_postgres = DatasetImplementationInfoFactory.construct_by_name(
        "pn_dict_postgres",
        test_app_info,
        pn_dict_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(pn_dict_postgres)

    pn_purchase_visit_id_dict_postgres = DatasetImplementationInfoFactory.construct_by_name(
        "pn_purchase_visit_id_dict_postgres",
        test_app_info,
        pn_purchase_visit_id_dict_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
        pn_purchase_visit_id_dict_postgres)

    # Driver позволяет считывать процессором данные из конкретного источника и записывать их в него.
    # Он привязан к processor backend и source и реализуется в виде класса, который наследует либо от
    # WriteDriver, либо от ReadDriver.
    # Разработчик процессора никогда не пользуется драйвером напрямую; вместо этого драйвер загружается
    # либо классом Reader, либо классом Writer, которые используются разработчиками для записи или
    # чтения данных соответственно.
    # Чтобы использовать конкретный драйвер, его нужно сначала зарегистрировать.
    sqlite_read_driver_info = DriverInfoFactory.construct_by_name("sqlite_read_driver",
                                         "on_read",
                                         dict(package_name="python_drivers.sqlite_read_driver",
                                              class_name="SqliteReadDriver"),
                                         sqlite_source_info,
                                         py37ds_processor_backend_info)
    DriverHandler().register(sqlite_read_driver_info)

    sqlite_write_driver_info = DriverInfoFactory.construct_by_name("sqlite_write_driver",
                                                                   "on_write",
                                                                   dict(package_name="python_drivers.sqlite_write_driver",
                                                                        class_name="SqliteWriteDriver"),
                                                                   sqlite_source_info,
                                                                   py37ds_processor_backend_info)
    DriverHandler().register(sqlite_write_driver_info)

    postgres_pdm_read_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_pdm_read_driver",
        "on_read",
        dict(package_name="python_drivers.postgres_pdm_read_driver",
             class_name="PostgresPdmReadDriver"),
        postgres_pdm_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_pdm_read_driver_info)

    postgres_pdm_write_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_pdm_write_driver",
        "on_write",
        dict(package_name="python_drivers.postgres_pdm_write_driver",
             class_name="PostgresPdmWriteDriver"),
        postgres_pdm_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_pdm_write_driver_info)

    postgres_pdm_localhost_read_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_pdm_localhost_read_driver",
        "on_read",
        dict(package_name="python_drivers.postgres_pdm_localhost_read_driver",
             class_name="PostgresPdmLocalhostReadDriver"),
        postgres_pdm_localhost_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_pdm_localhost_read_driver_info)

    postgres_pdm_localhost_write_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_pdm_localhost_write_driver",
        "on_write",
        dict(package_name="python_drivers.postgres_pdm_localhost_write_driver",
             class_name="PostgresPdmLocalhostWriteDriver"),
        postgres_pdm_localhost_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_pdm_localhost_write_driver_info)

    csv_metadata_read_driver_info = DriverInfoFactory.construct_by_name(
        "csv_metadata_read_driver_info",
        "on_read",
        dict(package_name="python_drivers.csv_read_driver", class_name="CsvReadDriver"),
        csv_metadata_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(csv_metadata_read_driver_info)

    csv_metadata_write_driver_info = DriverInfoFactory.construct_by_name(
        "csv_metadata_write_driver_info",
        "on_write",
        dict(package_name="python_drivers.csv_write_driver", class_name="CsvWriteDriver"),
        csv_metadata_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(csv_metadata_write_driver_info)

    # здесь мы регистрируем dataset implementations для partnumbers
    reader = Reader(APP_NAME, PROCESSOR_BACKEND_NAME)
    all_part_numbers = reader.read_by_draft_name("pn_dict")
    part_numbers = all_part_numbers["NAME_FOR_FILE"][:10].tolist()
    for part_number in part_numbers:
        part_number_draft_info = DraftInfoFactory.construct_by_name(part_number)

        part_number_dataset_implementation_name = f"{part_number}_postgres_pdm_localhost"
        part_number_postgres_pdm_localhost = DatasetImplementationInfoFactory.construct_by_name(
            part_number_dataset_implementation_name,
            test_app_info,
            part_number_draft_info,
            postgres_pdm_localhost_source_info
        )
        DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(
            part_number_postgres_pdm_localhost)
        DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(
            part_number_postgres_pdm_localhost)

    # Вместо того чтобы работать с airflow напрямую, фреймворк использует промежуточное представление графа в
    # графовой базе данных (neo4j). Чтобы зарегистрировать там граф, сначала нужно его собрать с помощью networkx.
    # Вершины графа - абстракрные процессоры; дуги - зависимости этих процессоров друг от друга.
    dag_flow_level = DiGraph()

    dag_flow_level.add_node(normalized_visits_abstract_processor_info)
    dag_flow_level.add_node(reversal_abstract_processor_info)
    dag_flow_level.add_node(binding_transactions_ambient_visits_abstract_processor_info)
    dag_flow_level.add_node(get_features_for_pn_abstract_processor_info)

    dag_flow_level.add_edge(binding_transactions_ambient_visits_abstract_processor_info,
                            normalized_visits_abstract_processor_info,
                            dependency=ProcessorUponProcessorDependencyInfoFactory.construct(
                                md5("normalized_visits-->binding_transactions_ambient_visits".encode("utf-8")).hexdigest(),
                                binding_transactions_ambient_visits_abstract_processor_info,
                                normalized_visits_abstract_processor_info))
    dag_flow_level.add_edge(binding_transactions_ambient_visits_abstract_processor_info,
                            reversal_abstract_processor_info,
                            dependency=ProcessorUponProcessorDependencyInfoFactory.construct(
                                md5("reversal-->binding_transactions_ambient_visits".encode("utf-8")).hexdigest(),
                                binding_transactions_ambient_visits_abstract_processor_info,
                                reversal_abstract_processor_info))
    dag_flow_level.add_edge(get_features_for_pn_abstract_processor_info,
                            binding_transactions_ambient_visits_abstract_processor_info,
                            dependency=ProcessorUponProcessorDependencyInfoFactory.construct(
                                md5("binding_transactions_ambient_visits-->get_features_for_pn".encode("utf-8")).hexdigest(),
                                get_features_for_pn_abstract_processor_info,
                                binding_transactions_ambient_visits_abstract_processor_info))

    test_dag_flow_level_info = DagFlowLevelInfoFactory.construct_by_name(
        DAG_FLOW_LEVEL_NAME,
        dag_flow_level
    )
    DagFlowLevelHandler().register(test_dag_flow_level_info)

    # Для того чтобы собрать dag, который можно использовать в аirflow, нужно сначала указать,
    # какие реализации процессоров следует использовать.
    test_abstract_processor_to_implementation_mapping_info = AbstractProcessorToImplementationMappingInfoFactory.construct(
        md5("test_dag_mapping".encode("utf-8")).hexdigest(),
        {normalized_visits_abstract_processor_info: normalized_visits_processor_implementation_info,
         reversal_abstract_processor_info: reversal_processor_implementation_info,
         binding_transactions_ambient_visits_abstract_processor_info: binding_transactions_ambient_visits_processor_implementation_info,
         get_features_for_pn_abstract_processor_info: get_features_for_pn_processor_implementation_info}
    )
    AbstractProcessorToImplementationMappingHandler().register(test_abstract_processor_to_implementation_mapping_info)

    # Dag implementation - это реализация dag'а, хранящегося в neo4j, с помощью конкретных процессоров, указанных
    # в mapping info.
    test_dag_implementation_info = DagImplementationInfoFactory.construct_by_name(
        "test_dag_implementation",
        test_dag_flow_level_info,
        test_abstract_processor_to_implementation_mapping_info
    )
    DagImplementationHandler().register(test_dag_implementation_info)

    # Чтобы запустить dag в airflow, нужно указать параметры сборки (запускать dag каждый день или однократно,
    # сколько раз пытаться запускать упавшие таски и так далее.
    test_dag_build_parameters = {
        "schedule_interval": "@once",
        "start_date": dt.datetime.now().strftime("%d.%m.%Y"),
        "default_args": {
            "owner": "eugene",
            "depends_on_past": True,
            "retries": 1
        }
    }
    test_dag_build_info = DagBuildInfoFactory.construct_by_name(
        "test_dag_build",
        test_app_info,
        test_dag_implementation_info,
        test_dag_build_parameters
    )
    DagBuildHandler().register(test_dag_build_info)

    # Чтобы даг появился папке airflow, его нужно собрать.
    DagBuilder(test_dag_build_info).write_dag()


if __name__ == "__main__":
    main()

