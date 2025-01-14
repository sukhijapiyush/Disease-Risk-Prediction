import os
import polars as pl
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path


def read_parquet_with_polars(file_path):
    """Reads a Parquet file into a Polars DataFrame."""
    return pl.read_parquet(file_path)


def get_common_columns_with_types(dataframes):
    """Finds common columns and ensures consistent types across all Polars DataFrames."""
    common_columns = set(dataframes[0].columns)
    for df in dataframes[1:]:
        common_columns.intersection_update(df.columns)

    # Infer and align types for common columns
    common_columns = list(common_columns)
    column_types = {}
    for column in common_columns:
        # Collect types of the column across all DataFrames
        types = set(df.schema.get(column, None) for df in dataframes)
        types.discard(None)  # Remove None if the column is missing in any DataFrame

        if len(types) == 1:
            column_types[column] = types.pop()  # Single type, no casting needed
        else:
            column_types[column] = pl.Utf8  # Default to string if multiple types exist

    return common_columns, column_types


def cast_dataframe_to_common_types(df, common_columns, column_types):
    """Casts a DataFrame's columns to the common types."""
    casted_columns = {col: df[col].cast(column_types[col]) for col in common_columns}
    return pl.DataFrame(casted_columns)


def write_parquet_chunk_with_polars(chunk, output_file):
    """Writes a Polars DataFrame chunk to a Parquet file."""
    chunk.write_parquet(output_file)
    print(f"Created: {output_file}")


def combine_and_split_parquet_with_polars(input_files, output_dir, chunk_size_mb):
    """
    Combines multiple Parquet files with consistent column types and splits them into smaller files.

    Parameters:
        input_files (list of str): List of input Parquet file paths.
        output_dir (str): Directory to store the output files.
        chunk_size_mb (int): Maximum size of each output file in MB.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Read all Parquet files in parallel
    with ProcessPoolExecutor() as executor:
        dataframes = list(executor.map(read_parquet_with_polars, input_files))

    # Identify common columns and their consistent types
    common_columns, column_types = get_common_columns_with_types(dataframes)

    # Cast all DataFrames to have consistent column types
    dataframes = [
        cast_dataframe_to_common_types(df, common_columns, column_types)
        for df in dataframes
    ]

    # Combine all DataFrames
    combined_df = pl.concat(dataframes)

    # Calculate the number of rows per chunk based on chunk size
    row_size = combined_df.estimated_size() / len(
        combined_df
    )  # Approximate size per row in bytes
    chunk_size_rows = int((chunk_size_mb * 1024 * 1024) / row_size)

    # Split the combined DataFrame into chunks
    chunks = [
        combined_df.slice(start_row, chunk_size_rows)
        for start_row in range(0, len(combined_df), chunk_size_rows)
    ]

    # Write chunks to Parquet in parallel
    with ProcessPoolExecutor() as executor:
        executor.map(
            write_parquet_chunk_with_polars,
            chunks,
            [
                os.path.join(output_dir, f"combined_part_{i+1}.parquet")
                for i in range(len(chunks))
            ],
        )


if __name__ == "__main__":
    # Paths to the input Parquet files
    input_files = [
        Path(r"data\Raw_Dataset\Original_Dataset\2013.parquet"),
        Path(r"data\Raw_Dataset\Original_Dataset\2014.parquet"),
        Path(r"data\Raw_Dataset\Original_Dataset\2015.parquet"),
    ]  # Replace with actual file paths

    # Output directory for smaller Parquet files
    output_dir = Path("data\Raw_Dataset\Data_Chunks")

    # Chunk size in MB
    chunk_size_mb = 5

    combine_and_split_parquet_with_polars(input_files, output_dir, chunk_size_mb)
