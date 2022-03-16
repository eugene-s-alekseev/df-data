import json

import pandas as pd
from sqlalchemy import create_engine

from src.data_model import DraftInfo, AppInfo
from src.driver_construction import WriteDriver


class SqliteAppendWriteDriver(WriteDriver):
    def __init__(self, driver_info):
        self.driver_info = driver_info

    def write(self, dataset: pd.DataFrame, draft_info: DraftInfo, app_info: AppInfo, **discovery_params) -> None:
        source_name = self.get_source_name(draft_info, app_info)
        connection_string = json.loads(self.driver_info.source_info.connection_info)["connection_string"]
        target_engine = create_engine(connection_string)

        dataset.to_sql(source_name, target_engine, if_exists="append", index=False)

    def get_source_name(self, draft_info: DraftInfo, app_info: AppInfo) -> str:
        return draft_info.draft_name + "_" + app_info.app_name