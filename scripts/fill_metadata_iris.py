from json import dumps

from src.config import path_to_sources, postgres_pdm_localhost_connection_string
from src.data_model import AbstractProcessorInfoFactory, AppInfoFactory, SourceInfoFactory, DraftInfoFactory, \
    ProcessorBackendInfoFactory, ProcessorImplementationInfoFactory, DatasetImplementationInfoFactory, \
    DriverInfoFactory, ModelDriverInfoFactory, ModelBackendInfoFactory, AbstractModelInfoFactory, \
    ModelUponInletDraftDependencyInfoFactory, ModelProducesOutletDraftInfoFactory, ModelImplementationInfoFactory, \
    ModelBackendsAvailableInfo
from src.registration import ProcessorImplementationHandler, DatasetImplementationHandlerFactory, DriverHandler, \
    ModelDriverHandler, AbstractProcessorHandler, AppHandler, SourceHandler, DraftHandler, ProcessorBackendHandler, \
    ModelBackendHandler, AbstractModelHandler, ModelUponInletDraftDependencyHandler, ModelProducesOutletDraftHandler, \
    ModelImplementationHandler, ModelBackendsAvailableHandler


def main():
    processor_backend_name = "py37"
    train_app_name = "iris_train"
    predict_app_name = "iris_predict"

    # abstract processors

    iris_train_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("iris_train")
    AbstractProcessorHandler().register(iris_train_abstract_processor_info)
    iris_predict_abstract_processor_info = AbstractProcessorInfoFactory.construct_by_name("iris_predict")
    AbstractProcessorHandler().register(iris_predict_abstract_processor_info)

    # app

    train_app_info = AppInfoFactory.construct_by_name(train_app_name)
    AppHandler().register(train_app_info)
    predict_app_info = AppInfoFactory.construct_by_name(predict_app_name)
    AppHandler().register(predict_app_info)

    # source

    postgres_pdm_localhost_connection_info = dumps(
        dict(connection_string=postgres_pdm_localhost_connection_string))
    postgres_pdm_localhost_source_info = SourceInfoFactory.construct_by_name("postgres-pdm-localhost",
                                                                             postgres_pdm_localhost_connection_info)

    # draft
    iris_features_draft_info = DraftInfoFactory.construct_by_name("iris_features")
    DraftHandler().register(iris_features_draft_info)
    iris_target_draft_info = DraftInfoFactory.construct_by_name("iris_target")
    DraftHandler().register(iris_target_draft_info)

    py37ds_processor_backend_info = ProcessorBackendInfoFactory.construct_by_name(processor_backend_name)
    ProcessorBackendHandler().register(py37ds_processor_backend_info)

    # processor implementations

    iris_train_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "iris_train_py37",
        iris_train_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(iris_train_processor_implementation_info)

    iris_predict_processor_implementation_info = ProcessorImplementationInfoFactory.construct_by_name(
        "iris_predict_py37",
        iris_predict_abstract_processor_info,
        py37ds_processor_backend_info
    )
    ProcessorImplementationHandler().register(iris_predict_processor_implementation_info)

    # dataset implementations

    iris_train_features_csv = DatasetImplementationInfoFactory.construct_by_name(
        "iris_train_features_postgres",
        train_app_info,
        iris_features_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(iris_train_features_csv)

    iris_train_target_csv = DatasetImplementationInfoFactory.construct_by_name(
        "iris_train_target_postgres",
        train_app_info,
        iris_target_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(iris_train_target_csv)

    iris_predict_features_csv = DatasetImplementationInfoFactory.construct_by_name(
        "iris_predict_features_postgres",
        predict_app_info,
        iris_features_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(iris_predict_features_csv)

    iris_predict_target_csv = DatasetImplementationInfoFactory.construct_by_name(
        "iris_predict_target_postgres",
        predict_app_info,
        iris_target_draft_info,
        postgres_pdm_localhost_source_info
    )
    DatasetImplementationHandlerFactory.get_read_handler().register_with_highest_priority(iris_predict_target_csv)
    DatasetImplementationHandlerFactory.get_write_handler().register_with_highest_priority(iris_predict_target_csv)

    # drivers

    postgres_read_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_read_driver_info",
        "on_read",
        dict(package_name="python_drivers.postgres_pdm_localhost_read_driver", class_name="PostgresPdmLocalhostReadDriver"),
        postgres_pdm_localhost_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_read_driver_info)

    postgres_write_driver_info = DriverInfoFactory.construct_by_name(
        "postgres_write_driver_info",
        "on_write",
        dict(package_name="python_drivers.postgres_pdm_localhost_write_driver", class_name="PostgresPdmLocalhostWriteDriver"),
        postgres_pdm_localhost_source_info,
        py37ds_processor_backend_info
    )
    DriverHandler().register(postgres_write_driver_info)

    sklearn_classifier_model_backend_info = ModelBackendInfoFactory.construct_by_name("sklearn_backend")
    ModelBackendHandler().register(sklearn_classifier_model_backend_info)

    # models and dependencies
    iris_abstract_model_info = AbstractModelInfoFactory.construct_by_name("iris_abstract_model")
    AbstractModelHandler().register(iris_abstract_model_info)

    iris_depends_on_features_info = ModelUponInletDraftDependencyInfoFactory.construct_with_auto_id(iris_abstract_model_info,
                                                                                                    iris_features_draft_info)
    ModelUponInletDraftDependencyHandler().register(iris_depends_on_features_info)

    iris_produces_target_info = ModelProducesOutletDraftInfoFactory.construct_with_auto_id(iris_abstract_model_info,
                                                                                           iris_target_draft_info)
    ModelProducesOutletDraftHandler().register(iris_produces_target_info)

    iris_model_implementation_info = ModelImplementationInfoFactory.construct_by_name("logistic_regression_baseline",
                                                                                      iris_abstract_model_info,
                                                                                      sklearn_classifier_model_backend_info)
    ModelImplementationHandler().register(iris_model_implementation_info)

    # model drivers
    sklearn_classifier_read_model_driver_info = ModelDriverInfoFactory.construct_by_name(
        "sklearn_classifier_model_read_driver",
        py37ds_processor_backend_info,
        sklearn_classifier_model_backend_info,
        "on_read"
    )
    ModelDriverHandler().register(sklearn_classifier_read_model_driver_info)

    sklearn_classifier_available_on_py37_info = ModelBackendsAvailableInfo(py37ds_processor_backend_info,
                                                                           sklearn_classifier_model_backend_info)
    ModelBackendsAvailableHandler().register(sklearn_classifier_available_on_py37_info)

    sklearn_classifier_write_model_driver_info = ModelDriverInfoFactory.construct_by_name(
        "sklearn_classifier_model_write_driver",
        py37ds_processor_backend_info,
        sklearn_classifier_model_backend_info,
        "on_write"
    )
    ModelDriverHandler().register(sklearn_classifier_write_model_driver_info)


if __name__ == "__main__":
    main()