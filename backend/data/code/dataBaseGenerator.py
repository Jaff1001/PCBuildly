import pandas as pd
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
load_dotenv()

BASE_PATH = Path(__file__).resolve().parent.parent 
DATA_PATH = BASE_PATH / "modified_data"
DB_URI = os.getenv("DB_URI")

# === MAPS === 
socket_map = {
    'AM5': 1, 'AM4': 2, 'LGA1700': 3, 'LGA1200': 4, 'LGA1851': 5
}

form_factor_map = {
    'ATX': 1, 'Micro ATX': 2, 'Mini ITX': 3
}

memory_type_map = {
    'DDR5': 1, 'DDR4': 2
}

# === TRANSLATORS ===
arch_to_socket_translator = {
    # AMD
    'Zen 5': 'AM5', 
    'Zen 4': 'AM5', 
    'Zen 3': 'AM4', 
    'Zen 2': 'AM4',
    
    # Intel
    'Arrow Lake': 'LGA1851',
    'Raptor Lake Refresh': 'LGA1700', 
    'Raptor Lake': 'LGA1700', 
    'Alder Lake': 'LGA1700',
    'Rocket Lake': 'LGA1200', 
    'Comet Lake': 'LGA1200'
}

socket_to_ram_translator = {
    'AM5': 'DDR5', 
    'LGA1851': 'DDR5', 
    'AM4': 'DDR4', 
    'LGA1700': 'DDR4', 
    'LGA1200': 'DDR4'
}

case_to_ff_translator = {
    'ATX Mid Tower': 'ATX', 
    'ATX Full Tower': 'ATX', 
    'MicroATX Mini Tower': 'Micro ATX', 
    'Mini ITX Tower': 'Mini ITX'
}

speed_to_ddr_translator = {'5': 'DDR5', '4': 'DDR4'}


# === INICIALIZATES DATABASE ===
def run_setup():
    connection = None
    try:
        connection = psycopg2.connect(DB_URI)
        cursor = connection.cursor()
        print("Connection with database establiced.")

        schema = """
        DROP TABLE IF EXISTS motherboards, cpus, video_cards, cases, memory, ups, cpu_coolers CASCADE;
        DROP TABLE IF EXISTS sockets, form_factors, memory_types CASCADE;

        CREATE TABLE sockets (
            id INTEGER PRIMARY KEY, 
            name TEXT UNIQUE
        );

        CREATE TABLE form_factors (
            id INTEGER PRIMARY KEY, 
            name TEXT UNIQUE
        );

        CREATE TABLE memory_types (
            id INTEGER PRIMARY KEY, 
            name TEXT UNIQUE
        );

        CREATE TABLE cpus (
            id SERIAL PRIMARY KEY, 
            name TEXT, 
            price DECIMAL, 
            socket_id INTEGER REFERENCES sockets(id), 
            tdp NUMERIC, 
            microarchitecture TEXT
        );

        CREATE TABLE motherboards (
            id SERIAL PRIMARY KEY,
            name TEXT,
            price DECIMAL, 
            socket_id INTEGER REFERENCES sockets(id), 
            form_factor_id INTEGER REFERENCES form_factors(id),
            memory_type_id INTEGER REFERENCES memory_types(id),
            max_memory NUMERIC, 
            memory_slots NUMERIC
        );

        CREATE TABLE video_cards (
            id SERIAL PRIMARY KEY, 
            name TEXT, 
            price DECIMAL, 
            chipset TEXT, 
            memory_gb NUMERIC, 
            length_mm NUMERIC
        );

        CREATE TABLE cases (
            id SERIAL PRIMARY KEY, 
            name TEXT, 
            price DECIMAL, 
            type TEXT, 
            form_factor_id INTEGER REFERENCES form_factors(id),
            max_gpu_length_mm NUMERIC
        );

        CREATE TABLE memory (
            id SERIAL PRIMARY KEY, 
            name TEXT, 
            price DECIMAL, 
            memory_type_id INTEGER REFERENCES memory_types(id), 
            speed_text TEXT, 
            cas_latency NUMERIC
        );

        CREATE TABLE ups (
            id SERIAL PRIMARY KEY, 
            name TEXT, 
            price DECIMAL, 
            capacity_watts NUMERIC
        );
        """
        cursor.execute(schema)

        print("Inserting catalog data...")

        for name, id_val in socket_map.items():
            cursor.execute("INSERT INTO sockets (id, name) VALUES (%s, %s)", (id_val, name))
        for name, id_val in form_factor_map.items():
            cursor.execute("INSERT INTO form_factors (id, name) VALUES (%s, %s)", (id_val, name))
        for name, id_val in memory_type_map.items():
            cursor.execute("INSERT INTO memory_types (id, name) VALUES (%s, %s)", (id_val, name))

        print("Uploading components...")

        # Cpus
        cpu_df = pd.read_csv(DATA_PATH / 'clear_cpu.csv')
        for _, row in cpu_df.iterrows():
            arch = row['microarchitecture']
            socket_name = arch_to_socket_translator.get(arch)
            s_id = socket_map[socket_name]
            cursor.execute("INSERT INTO cpus (name, price, socket_id, tdp, microarchitecture) VALUES (%s, %s, %s, %s, %s)",
                            (row['name'], row['price'], s_id, row['tdp'], arch))
    
        # Motherboards
        mobo_df = pd.read_csv(DATA_PATH / 'clear_motherboard.csv')
        for _, row in mobo_df.iterrows():
            s_name = row['socket']
            ram_type = socket_to_ram_translator.get(s_name)
            cursor.execute("INSERT INTO motherboards (name, price, socket_id, form_factor_id, memory_type_id, max_memory, memory_slots) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (row['name'], row['price'], socket_map[s_name], form_factor_map[row['form_factor']], memory_type_map[ram_type], row['max_memory'], row['memory_slots']))
        
        # Video cards
        gpu_df = pd.read_csv(DATA_PATH / 'clear_video-card.csv')
        for _, row in gpu_df.iterrows():
            cursor.execute("INSERT INTO video_cards (name, price, chipset, memory_gb, length_mm) VALUES (%s, %s, %s, %s, %s)",
                          (row['name'], row['price'], row['chipset'], row['memory'], row['length']))

        # Cases
        case_df = pd.read_csv(DATA_PATH / 'clear_case.csv')
        for _, row in case_df.iterrows():
            ff_name = case_to_ff_translator.get(row['type'], 'ATX') 
            cursor.execute("INSERT INTO cases (name, price, type, form_factor_id) VALUES (%s, %s, %s, %s)",
                          (row['name'], row['price'], row['type'], form_factor_map[ff_name]))

        # Memory
        mem_df = pd.read_csv(DATA_PATH / 'clear_memory.csv')
        for _, row in mem_df.iterrows():
            speed_str = str(row['speed'])
            ram_type = speed_to_ddr_translator.get(speed_str[0])
            cursor.execute("INSERT INTO memory (name, price, memory_type_id, speed_text, cas_latency) VALUES (%s, %s, %s, %s, %s)",
                            (row['name'], row['price'], memory_type_map[ram_type], speed_str, row['cas_latency']))
    
        connection.commit()
        print("Database correctly generated.")

    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    run_setup()