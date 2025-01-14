import pandas as pd
from pycaret.classification import *
from preprocessing_utils_constant import *


# Data loader function
def load_data(folder_path: str) -> pd.DataFrame:
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Dataset not found at path {folder_path}")
    else:
        df = load_parquets(folder_path)
        return df
