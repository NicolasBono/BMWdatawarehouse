import os

# Rutas
INPUT_FILE = 'bmw_sales.csv'
OUTPUT_DIR = 'outputs'
TEMP_DIR = 'temp'

# Crear directorios
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
