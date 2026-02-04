import re
import numpy as np
import pandas as pd
import unicodedata
from collections import defaultdict

class Standardizer:
    
    @staticmethod
    def rename_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        column_names = []
        counts = defaultdict(int)
        for original in dataframe.columns:
            c = unicodedata.normalize("NFKD", original) \
                .encode("ASCII", "ignore").decode("ASCII") \
                .replace(" ", "_").replace(".", "_").replace("/", "_").replace(",", "_")
            c = re.sub(r"[^0-9a-zA-Z$]+", "_", c)
            c = re.sub(r"_+", "_", c).strip("_")
            c = c.upper()
            counts[c] += 1
            if counts[c] > 1:
                c = f"{c}_{counts[c]}"
            column_names.append(c)
        dataframe.columns = column_names
        return dataframe
    
    @staticmethod
    def set_empty_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        for col in dataframe.columns:
            if dataframe[col].dtype == object:
                dataframe[col] = dataframe[col].astype(str).str.strip()
        return dataframe.replace("-", None).replace("None", None)
    
    @staticmethod
    def drop_empty_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe.dropna(axis=1, how="all")
    
    def execute(self, dataframe:pd.DataFrame) -> pd.DataFrame:
        dataframe = self.rename_columns(dataframe)
        dataframe = self.set_empty_columns(dataframe)
        dataframe = self.drop_empty_columns(dataframe)
        return dataframe
    
