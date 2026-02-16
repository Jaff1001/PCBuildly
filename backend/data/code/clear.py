import os
import csv
from pathlib import Path
from tqdm import tqdm
import pandas as pd


BASE_PATH = Path(__file__).resolve().parent.parent

folder_path = BASE_PATH / "original_data"
output_path = BASE_PATH / "modified_data"

# === CLEAR THE FILES TO OBTAIN THE FINAL FILE WITH PROCES NOT NULLS ===
def cleanFiles():
    for csv_file in folder_path.glob("*.csv"):
        df = pd.read_csv(csv_file)

        if "price" in df.columns:
            print(f"Porcessing the csv with name: {csv_file.name}.")

            initial_lines = len(df)
            df_clear = df.dropna(subset=["price"])
            df_clear.insert(0, 'id', range(1, len(df_clear) + 1))
            deleted_lines = len(df_clear)
        
            new_name = f"clear_{csv_file.name}"
            df_clear.to_csv(output_path / new_name, index=False)

            print(f"Deleted {initial_lines - deleted_lines} files.")

        else:
            print(f"The csv with name: {csv_file.name} dosent have a column with name price.")

if __name__ == "__main__":
    cleanFiles()