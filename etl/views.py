from config import OUTPUT_DIR
from utils import log, print_header


def paso4_crear_vistas(hechos, dimensions):
    """Crea las 6 vistas analíticas"""
    print_header("PASO 4: CREACIÓN DE VISTAS ANALÍTICAS")

    # Crear vista completa (uniendo todas las dimensiones)
    log("Creando vista completa...", "PROCESS")

    vista = hechos.copy()
    vista = vista.merge(dimensions['tiempo'][['id_tiempo', 'anio', 'trimestre', 'semestre', 'decada']], on='id_tiempo')
    vista = vista.merge(dimensions['region'][['id_region', 'nombre_region']], on='id_region')
    vista = vista.merge(dimensions['combustible'][['id_combustible', 'tipo_combustible']], on='id_combustible')
    vista = vista.merge(dimensions['transmision'][['id_transmision', 'tipo_transmision']], on='id_transmision')

    # ── VISTA 1: Ventas por Año y Región ──────────────────────────────────
    log("Creando VISTA 1: Ventas por Año y Región...", "PROCESS")

    v1 = vista.groupby(['anio', 'nombre_region']).agg({
        'cantidad_unidades': 'sum',
        'monto_total': 'sum',
        'precio_unitario': 'mean'
    }).reset_index()
    v1.columns = ['anio', 'region', 'total_unidades', 'monto_total_ventas', 'precio_promedio']
    v1['monto_total_ventas'] = v1['monto_total_ventas'].round(2)
    v1['precio_promedio'] = v1['precio_promedio'].round(2)
    v1.to_csv(f"{OUTPUT_DIR}/VISTA_VentasPorAnioYRegion.csv", index=False)

    # ── VISTA 2: Participación de Eléctricos/Híbridos ─────────────────────
    log("Creando VISTA 2: Participación Eléctricos...", "PROCESS")

    vista['categoria_combustible'] = vista['tipo_combustible'].apply(
        lambda x: 'Alternativo' if x in ['Electric', 'Hybrid'] else 'Tradicional'
    )

    totales = vista.groupby(['anio', 'nombre_region'])['cantidad_unidades'].sum().reset_index()
    totales.rename(columns={'cantidad_unidades': 'total_unidades'}, inplace=True)

    alternativos = vista[vista['categoria_combustible'] == 'Alternativo'].groupby(
        ['anio', 'nombre_region']
    )['cantidad_unidades'].sum().reset_index()
    alternativos.rename(columns={'cantidad_unidades': 'unidades_alternativas'}, inplace=True)

    v2 = totales.merge(alternativos, on=['anio', 'nombre_region'], how='left')
    v2['unidades_alternativas'] = v2['unidades_alternativas'].fillna(0)
    v2['porcentaje_alternativo'] = (v2['unidades_alternativas'] / v2['total_unidades'] * 100).round(2)
    v2['unidades_tradicionales'] = v2['total_unidades'] - v2['unidades_alternativas']
    v2['porcentaje_tradicional'] = (v2['unidades_tradicionales'] / v2['total_unidades'] * 100).round(2)
    v2.rename(columns={'nombre_region': 'region'}, inplace=True)
    v2.to_csv(f"{OUTPUT_DIR}/VISTA_ParticipacionElectricos.csv", index=False)

    # ── VISTA 3: Crecimiento Año sobre Año ────────────────────────────────
    log("Creando VISTA 3: Crecimiento YoY...", "PROCESS")

    v3 = vista.groupby(['anio', 'nombre_region'])['cantidad_unidades'].sum().reset_index()
    v3 = v3.sort_values(['nombre_region', 'anio'])
    v3['unidades_año_anterior'] = v3.groupby('nombre_region')['cantidad_unidades'].shift(1)
    v3['incremento_unidades'] = v3['cantidad_unidades'] - v3['unidades_año_anterior']
    v3['crecimiento_porcentual'] = ((v3['cantidad_unidades'] - v3['unidades_año_anterior']) /
                                     v3['unidades_año_anterior'] * 100).round(2)
    v3.rename(columns={'nombre_region': 'region', 'cantidad_unidades': 'unidades_año_actual'}, inplace=True)
    v3 = v3[v3['unidades_año_anterior'].notna()]
    v3.to_csv(f"{OUTPUT_DIR}/VISTA_CrecimientoYoY.csv", index=False)

    # ── VISTA 4: Ventas por Tipo de Combustible ───────────────────────────
    log("Creando VISTA 4: Ventas por Combustible...", "PROCESS")

    v4 = vista.groupby(['anio', 'tipo_combustible']).agg({
        'cantidad_unidades': 'sum',
        'monto_total': 'sum',
        'precio_unitario': 'mean'
    }).reset_index()
    v4.columns = ['anio', 'tipo_combustible', 'total_unidades', 'monto_total_ventas', 'precio_promedio']

    totales_año = v4.groupby('anio')['total_unidades'].sum().reset_index()
    totales_año.rename(columns={'total_unidades': 'total_año'}, inplace=True)
    v4 = v4.merge(totales_año, on='anio')
    v4['participacion_porcentual'] = (v4['total_unidades'] / v4['total_año'] * 100).round(2)
    v4 = v4[['anio', 'tipo_combustible', 'total_unidades', 'participacion_porcentual',
             'monto_total_ventas', 'precio_promedio']]
    v4['monto_total_ventas'] = v4['monto_total_ventas'].round(2)
    v4['precio_promedio'] = v4['precio_promedio'].round(2)
    v4.to_csv(f"{OUTPUT_DIR}/VISTA_VentasPorCombustible.csv", index=False)

    # ── VISTA 5: Ventas por Tipo de Transmisión ───────────────────────────
    log("Creando VISTA 5: Ventas por Transmisión...", "PROCESS")

    v5 = vista.groupby(['anio', 'tipo_transmision']).agg({
        'cantidad_unidades': 'sum',
        'monto_total': 'sum'
    }).reset_index()
    v5.columns = ['anio', 'tipo_transmision', 'total_unidades', 'monto_total_ventas']

    totales_año = v5.groupby('anio')['total_unidades'].sum().reset_index()
    totales_año.rename(columns={'total_unidades': 'total_año'}, inplace=True)
    v5 = v5.merge(totales_año, on='anio')
    v5['participacion_porcentual'] = (v5['total_unidades'] / v5['total_año'] * 100).round(2)
    v5 = v5[['anio', 'tipo_transmision', 'total_unidades', 'participacion_porcentual', 'monto_total_ventas']]
    v5['monto_total_ventas'] = v5['monto_total_ventas'].round(2)
    v5.to_csv(f"{OUTPUT_DIR}/VISTA_VentasPorTransmision.csv", index=False)

    # ── VISTA 6: KPIs ─────────────────────────────────────────────────────
    log("Creando VISTA 6: KPIs...", "PROCESS")

    v6 = vista.groupby('anio').agg({
        'cantidad_unidades': 'sum',
        'monto_total': 'sum',
        'precio_unitario': 'mean'
    }).reset_index()
    v6.columns = ['anio', 'total_unidades', 'monto_total_ventas', 'precio_promedio']
    v6['unidades_año_anterior'] = v6['total_unidades'].shift(1)
    v6['crecimiento_yoy_pct'] = ((v6['total_unidades'] - v6['unidades_año_anterior']) /
                                  v6['unidades_año_anterior'] * 100).round(2)
    v6['monto_total_ventas'] = v6['monto_total_ventas'].round(2)
    v6['precio_promedio'] = v6['precio_promedio'].round(2)
    v6 = v6[['anio', 'total_unidades', 'monto_total_ventas', 'precio_promedio', 'crecimiento_yoy_pct']]
    v6.to_csv(f"{OUTPUT_DIR}/VISTA_KPIs.csv", index=False)

    log("Todas las vistas creadas exitosamente", "SUCCESS")
