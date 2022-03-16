import json

import pandas as pd
from sqlalchemy import create_engine

from src.data_model import AppInfo, DraftInfo
from src.driver_construction import ReadDriver


class PostgresPdmReadDriver(ReadDriver):
    def __init__(self, driver_info):
        self.driver_info = driver_info

    def read(self, draft_info: DraftInfo, app_info: AppInfo, **discovery_params) -> pd.DataFrame:
        source_name = self.get_source_name(draft_info, app_info)
        connection_string = json.loads(self.driver_info.source_info.connection_info)["connection_string"]
        target_engine = create_engine(connection_string)
        dataset = pd.read_sql_table(source_name, target_engine)
        dataset.columns = [col.upper() for col in dataset]

        return dataset

    def get_source_name(self, draft_info: DraftInfo, app_info: AppInfo):
        return "dl." + "_" + draft_info.draft_name + "_" + app_info.app_name