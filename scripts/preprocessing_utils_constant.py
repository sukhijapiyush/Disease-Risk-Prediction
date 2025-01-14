from pathlib import Path
import os
import pandas as pd

RAW_DATA_CHUNKS_PATH = Path("data/Raw_Dataset/Data_Chunks")

PROCCESSED_DATA_PATH = Path("data/Processed_Dataset")


# Function to check if folder exists
def check_folder(folder_list: list) -> None:
    for folder in folder_list:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Created: {folder}")
        else:
            print(f"Folder already exists: {folder}")
    return None


# Function to load parquets in a folder and return a combined dataframe
def load_parquets(folder_path: Path) -> pd.DataFrame:
    parquet_files = list(folder_path.glob("*.parquet"))
    df = pd.read_parquet(parquet_files[0])
    for file in parquet_files[1:]:
        df = pd.concat([df, pd.read_parquet(file)], axis=0)
    return df
