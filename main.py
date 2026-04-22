"""
ETL BMW Data Warehouse
Proceso completo de ETL siguiendo la metodología HEFESTO.
Autores: Alfei Mateo, Bono Nicolas, Bocco Matias Juan
"""

from datetime import datetime

from config import OUTPUT_DIR
from utils import print_header
from etl import (
    paso1_extraccion,
    paso2_crear_dimensiones,
    paso3_crear_hechos,
    paso4_crear_vistas,
)


def main():
    """Ejecuta todo el proceso ETL"""
    print_header("ETL BMW DATA WAREHOUSE - METODOLOGIA HEFESTO")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Autores: Alfei Mateo, Bono Nicolas, Bocco Matias Juan\n")

    # PASO 1: Extraccion
    df = paso1_extraccion()
    if df is None:
        return

    # PASO 2: Dimensiones
    dimensions = paso2_crear_dimensiones(df)

    # PASO 3: Hechos
    hechos = paso3_crear_hechos(df, dimensions)

    # PASO 4: Vistas
    paso4_crear_vistas(hechos, dimensions)

    # Resumen final
    print_header("ETL COMPLETADO EXITOSAMENTE")
    print(f"4 dimensiones creadas")
    print(f"1 tabla de hechos con {len(hechos)} registros")
    print(f"6 vistas analiticas generadas")
    print(f"\nTodos los archivos estan en: {OUTPUT_DIR}/")
    print(f"Listo para importar a Power BI\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        traceback.print_exc()
