import polars as pl
from pathlib import Path


def convert_to_parquet(input_file, output_file):
    """Converts a CSV file to Parquet format."""
    df = pl.read_csv(input_file)
    df.write_parquet(output_file)
    print(f"Created: {output_file}")


convert_to_parquet(
    r"data\Raw_Dataset\Original_Dataset\2013.csv",
    r"data\Raw_Dataset\Original_Dataset\2013.parquet",
)
convert_to_parquet(
    r"data\Raw_Dataset\Original_Dataset\2014.csv",
    r"data\Raw_Dataset\Original_Dataset\2014.parquet",
)
convert_to_parquet(
    r"data\Raw_Dataset\Original_Dataset\2015.csv",
    r"data\Raw_Dataset\Original_Dataset\2015.parquet",
)
