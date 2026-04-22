import pandas as pd

from config import INPUT_FILE, TEMP_DIR
from utils import log, print_header


def paso1_extraccion():
    """Extrae y valida los datos del dataset original"""
    print_header("PASO 1: EXTRACCIÓN DE DATOS")

    log("Cargando dataset original...")

    try:
        df = pd.read_csv(INPUT_FILE)
        log(f"Dataset cargado: {len(df):,} registros", "SUCCESS")

        # Validaciones
        log("Ejecutando validaciones...", "PROCESS")

        nulos = df.isnull().sum().sum()
        duplicados = df.duplicated().sum()

        if nulos == 0:
            log("No se encontraron valores nulos", "SUCCESS")
        else:
            log(f"Se encontraron {nulos} valores nulos", "WARNING")

        if duplicados == 0:
            log("No se encontraron duplicados", "SUCCESS")
        else:
            log(f"Se encontraron {duplicados} duplicados", "WARNING")

        # Agregar ID único
        df.insert(0, 'id', range(1, len(df) + 1))

        # Guardar temporalmente
        df.to_csv(f"{TEMP_DIR}/bmw_sales_with_id.csv", index=False)

        log("Extracción completada", "SUCCESS")
        return df

    except Exception as e:
        log(f"Error en extracción: {str(e)}", "ERROR")
        return None
