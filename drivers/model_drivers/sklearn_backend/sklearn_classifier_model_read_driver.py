from typing import List, Any

import pandas as pd
from sklearn.base import ClassifierMixin
from sklearn.externals import joblib

from src.data_model import ModelImplementationInfo, ProcessorBackendInfo, ModelUponInletDraftDependencyInfo, \
    ModelProducesOutletDraftInfo
from src.model_drivers import ModelImplementation, ModelReadDriver, ModelWriteDriver
from src.readers import Reader, Writer
from src.retrievers import ModelUponInletDraftDependencyInfoRetriever, ModelProducesOutletDraftInfoRetriever


class Implementation(ModelImplementation):
    def __init__(self,
                 model_implementation_info: ModelImplementationInfo,
                 processor_backend_info: ProcessorBackendInfo,
                 model: ClassifierMixin):
        super().__init__(model_implementation_info,
                         processor_backend_info,
                         model)

    def predict_by_app_name(self, app_name: str) -> None:
        reader = Reader(app_name,
                        self.processor_backend_info.processor_backend_name)
        inlet_dependencies: List[ModelUponInletDraftDependencyInfo] = ModelUponInletDraftDependencyInfoRetriever.get_all_dependencies(
            self.model_implementation_info.implements_abstract_model.abstract_model_id
        )
        feature_draft_id = inlet_dependencies[0].depends_on_inlet_draft.draft_id
        features = reader.read_by_draft_id(feature_draft_id)

        prediction = pd.Series(self.model.predict(features)).to_frame("species")

        writer = Writer(app_name,
                        self.processor_backend_info.processor_backend_name)
        outlet_dependencies: List[ModelProducesOutletDraftInfo] = ModelProducesOutletDraftInfoRetriever.get_all_dependencies(
            self.model_implementation_info.implements_abstract_model.abstract_model_id
        )
        prediction_draft_id = outlet_dependencies[0].produces_outlet_draft.draft_id
        writer.write_by_draft_id(prediction, prediction_draft_id)

    def predict_by_app_id(self, app_id: str) -> None:
        pass


class ModelReadDriverImplementation(ModelReadDriver):
    def read(self, model_implementation_info: ModelImplementationInfo) -> Implementation:
        model = joblib.load(self.path_to_model(model_implementation_info))

        return Implementation(model_implementation_info,
                              self.model_driver_info.processor_backend_info,
                              model)


class ModelWriteDriverImplementation(ModelWriteDriver):
    def write(self,
              model: Any,
              model_implementation_info: ModelImplementationInfo):
        joblib.dump(model, self.path_to_model(model_implementation_info))