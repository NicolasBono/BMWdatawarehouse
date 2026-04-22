import pandas as pd
import pickle

from config import OUTPUT_DIR, TEMP_DIR
from utils import log, print_header


def paso2_crear_dimensiones(df):
    """Crea las 4 tablas dimensionales"""
    print_header("PASO 2: CREACIÓN DE DIMENSIONES")

    dimensions = {}

    # ── DIMENSIÓN TIEMPO ──────────────────────────────────────────────────
    log("Creando DIM_TIEMPO...", "PROCESS")

    años = sorted(df['Year'].unique())
    dim_tiempo_data = []

    for año in años:
        for trimestre_num in range(1, 5):
            id_tiempo = (año * 10) + trimestre_num
            trimestre = f"{trimestre_num}er Trim" if trimestre_num in [1, 3] else f"{trimestre_num}do Trim"
            semestre = "1er Semestre" if trimestre_num <= 2 else "2do Semestre"
            decada = (año // 10) * 10

            dim_tiempo_data.append({
                'id_tiempo': id_tiempo,
                'anio': año,
                'trimestre': trimestre,
                'semestre': semestre,
                'decada': decada
            })

    dimensions['tiempo'] = pd.DataFrame(dim_tiempo_data)
    dimensions['tiempo'].to_csv(f"{OUTPUT_DIR}/DIM_TIEMPO.csv", index=False)
    log(f"DIM_TIEMPO creada: {len(dimensions['tiempo'])} registros", "SUCCESS")

    # ── DIMENSIÓN REGIÓN ──────────────────────────────────────────────────
    log("Creando DIM_REGION...", "PROCESS")

    regiones = sorted(df['Region'].unique())
    dimensions['region'] = pd.DataFrame({
        'id_region': range(1, len(regiones) + 1),
        'nombre_region': regiones
    })
    dimensions['region'].to_csv(f"{OUTPUT_DIR}/DIM_REGION.csv", index=False)
    log(f"DIM_REGION creada: {len(dimensions['region'])} registros", "SUCCESS")

    # ── DIMENSIÓN COMBUSTIBLE ─────────────────────────────────────────────
    log("Creando DIM_COMBUSTIBLE...", "PROCESS")

    combustibles = sorted(df['Fuel_Type'].unique())
    dimensions['combustible'] = pd.DataFrame({
        'id_combustible': range(1, len(combustibles) + 1),
        'tipo_combustible': combustibles
    })
    dimensions['combustible'].to_csv(f"{OUTPUT_DIR}/DIM_COMBUSTIBLE.csv", index=False)
    log(f"DIM_COMBUSTIBLE creada: {len(dimensions['combustible'])} registros", "SUCCESS")

    # ── DIMENSIÓN TRANSMISIÓN ─────────────────────────────────────────────
    log("Creando DIM_TRANSMISION...", "PROCESS")

    transmisiones = sorted(df['Transmission'].unique())
    dimensions['transmision'] = pd.DataFrame({
        'id_transmision': range(1, len(transmisiones) + 1),
        'tipo_transmision': transmisiones
    })
    dimensions['transmision'].to_csv(f"{OUTPUT_DIR}/DIM_TRANSMISION.csv", index=False)
    log(f"DIM_TRANSMISION creada: {len(dimensions['transmision'])} registros", "SUCCESS")

    # Guardar en pickle para uso posterior
    with open(f"{TEMP_DIR}/dimensions.pkl", 'wb') as f:
        pickle.dump(dimensions, f)

    return dimensions


def paso3_crear_hechos(df, dimensions):
    """Crea la tabla de hechos agregada"""
    print_header("PASO 3: CREACIÓN DE TABLA DE HECHOS")

    log("Agregando datos...", "PROCESS")

    # Calcular revenue para cada registro
    df['revenue'] = df['Sales_Volume'] * df['Price_USD']

    # Agregar por las 4 dimensiones
    hechos = df.groupby(['Year', 'Region', 'Fuel_Type', 'Transmission']).agg({
        'Sales_Volume': 'sum',
        'revenue': 'sum',
        'Price_USD': 'mean'
    }).reset_index()

    hechos.columns = ['Year', 'Region', 'Fuel_Type', 'Transmission',
                      'cantidad_unidades', 'monto_total', 'precio_unitario']

    # Vincular con dimensiones
    log("Vinculando con Foreign Keys...", "PROCESS")

    # ID_TIEMPO (usando primer trimestre de cada año)
    tiempo_map = dimensions['tiempo'][dimensions['tiempo']['trimestre'] == '1er Trim'][['id_tiempo', 'anio']]
    hechos = hechos.merge(tiempo_map, left_on='Year', right_on='anio', how='left')
    hechos.drop(['Year', 'anio'], axis=1, inplace=True)

    # ID_REGION
    hechos = hechos.merge(dimensions['region'], left_on='Region', right_on='nombre_region', how='left')
    hechos.drop(['Region', 'nombre_region'], axis=1, inplace=True)

    # ID_COMBUSTIBLE
    hechos = hechos.merge(dimensions['combustible'], left_on='Fuel_Type', right_on='tipo_combustible', how='left')
    hechos.drop(['Fuel_Type', 'tipo_combustible'], axis=1, inplace=True)

    # ID_TRANSMISION
    hechos = hechos.merge(dimensions['transmision'], left_on='Transmission', right_on='tipo_transmision', how='left')
    hechos.drop(['Transmission', 'tipo_transmision'], axis=1, inplace=True)

    # Reorganizar columnas
    hechos = hechos[[
        'id_tiempo', 'id_region', 'id_combustible', 'id_transmision',
        'cantidad_unidades', 'monto_total', 'precio_unitario'
    ]]

    # Redondear
    hechos['monto_total'] = hechos['monto_total'].round(2)
    hechos['precio_unitario'] = hechos['precio_unitario'].round(2)

    # Guardar
    hechos.to_csv(f"{OUTPUT_DIR}/HECHOS_VENTAS.csv", index=False)
    log(f"HECHOS_VENTAS creada: {len(hechos)} registros", "SUCCESS")

    return hechos
