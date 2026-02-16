import os
import csv
from pathlib import Path
from tqdm import tqdm
import pandas as pd

# === VARIABLES THAT REPRESENT THE FILE PATHS ===
BASE_PATH = Path(__file__).resolve().parent.parent
folder_path = BASE_PATH / "original_data"
output_path = BASE_PATH / "modified_data"

# === ALLOWED VALUES (Filters) ===
allowed_sockets = ['AM5', 'AM4', 'LGA1700', 'LGA1200', 'LGA1851']

allowed_architectures = [
    'Zen 5', 'Zen 4', 'Zen 3', 'Zen 2',
    'Arrow Lake', 'Raptor Lake Refresh', 'Raptor Lake', 
    'Alder Lake', 'Rocket Lake', 'Comet Lake'
]

allowed_case_types = [
    'ATX Mid Tower', 
    'ATX Full Tower', 
    'MicroATX Mini Tower', 
    'Mini ITX Tower'
]

allowed_speed_prefixes = ('4', '5')

allowed_form_factors = ['ATX', 'Micro ATX', 'Mini ITX']

# === CLEAR THE FILES TO OBTAIN THE FINAL FILE WITH PROCES NOT NULLS ===
def cleanFiles():
    for csv_file in folder_path.glob("*.csv"):
        file_name = csv_file.name
        df = pd.read_csv(csv_file)

        if "price" in df.columns:
            print(f"Porcessing the csv with name: {csv_file.name}.")

            initial_lines = len(df)
            df = df.dropna(subset=["price"])

            # Eliminate NONE values and allowed values
            if file_name == "cpu.csv":
                df = df.dropna(subset=["microarchitecture", "tdp"])
                df = df[df['microarchitecture'].isin(allowed_architectures)]

            elif file_name == "motherboard.csv":
                df = df.dropna(subset=["socket", "form_factor", "max_memory", "memory_slots"])
                df = df[df['socket'].isin(allowed_sockets)]
                df = df[df['form_factor'].isin(allowed_form_factors)]

            elif file_name == "case.csv":
                df = df[df['type'].isin(allowed_case_types)]

            elif file_name == "memory.csv":
                df = df.dropna(subset=["speed", "cas_latency"])
                df = df[df['speed'].astype(str).str.startswith(allowed_speed_prefixes)]

            elif file_name == "case.csv":
                df = df[df['type'].isin(allowed_case_types)]

            elif file_name == "ups.csv":
                df = df.dropna(subset=["capacity_w"])

            df.insert(0, 'id', range(1, len(df) + 1))
            deleted_lines = len(df)
        
            new_name = f"clear_{csv_file.name}"
            df.to_csv(output_path / new_name, index=False)

            print(f"Deleted {initial_lines - deleted_lines} files.")

        else:
            print(f"The csv with name: {csv_file.name} dosent have a column with name price.")

if __name__ == "__main__":
    cleanFiles()