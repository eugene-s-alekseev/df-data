import json
import os

import pandas as pd

from src.data_model import DriverInfo, DraftInfo, AppInfo
from src.driver_construction import WriteDriver


class CsvAppendWriteDriver(WriteDriver):
    def __init__(self, driver_info: DriverInfo):
        self.driver_info = driver_info

    def write(self, dataset: pd.DataFrame, draft_info: DraftInfo, app_info: AppInfo, **discovery_params) -> None:
        file_name = self.get_source_name(draft_info, app_info)
        path_to_folder = json.loads(self.driver_info.source_info.connection_info)["path_to_data"]
        full_path = os.path.join(path_to_folder, file_name)

        dataset.to_csv(full_path, mode="a", index=False, header=list(dataset.columns))

    def get_source_name(self, draft_info: DraftInfo, app_info: AppInfo) -> str:
        return f"{draft_info.draft_name}_{app_info.app_name}.csv"