# BMW Data Warehouse - ETL Pipeline

Pipeline ETL en Python que transforma 50,000 registros de ventas de BMW en un Data Warehouse dimensional (modelo estrella), siguiendo la **metodología HEFESTO**.

## Descripcion del Proyecto

Este proyecto implementa un proceso completo de **Extract, Transform, Load (ETL)** que toma datos crudos de ventas de vehiculos BMW y los estructura en un modelo de Data Warehouse listo para analisis en herramientas de BI como Power BI.

### Que hace el pipeline

1. **Extraccion** - Lee y valida 50,000 registros de ventas desde un CSV
2. **Transformacion** - Construye un modelo estrella con 4 dimensiones y 1 tabla de hechos
3. **Carga** - Genera 6 vistas analiticas listas para consumir desde Power BI

## Arquitectura - Modelo Estrella

```
                    ┌──────────────┐
                    │  DIM_TIEMPO  │
                    │  (60 reg.)   │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴───────┐   ┌────────────────┐
│  DIM_REGION  ├───┤    HECHOS    ├───┤ DIM_COMBUSTIBLE│
│   (6 reg.)   │   │   VENTAS     │   │    (4 reg.)    │
└──────────────┘   │  (720 reg.)  │   └────────────────┘
                   └──────┬───────┘
                          │
                  ┌───────┴────────┐
                  │DIM_TRANSMISION │
                  │    (2 reg.)    │
                  └────────────────┘
```

### Dimensiones

| Dimension | Registros | Atributos |
|-----------|-----------|-----------|
| **DIM_TIEMPO** | 60 | anio, trimestre, semestre, decada |
| **DIM_REGION** | 6 | Africa, Asia, Europe, Middle East, North America, South America |
| **DIM_COMBUSTIBLE** | 4 | Diesel, Electric, Hybrid, Petrol |
| **DIM_TRANSMISION** | 2 | Automatic, Manual |

### Tabla de Hechos

**HECHOS_VENTAS** (720 registros) - Metricas agregadas por las 4 dimensiones:
- `cantidad_unidades` - Volumen total de ventas
- `monto_total` - Revenue (volumen x precio)
- `precio_unitario` - Precio promedio

### Vistas Analiticas

| Vista | Descripcion |
|-------|-------------|
| Ventas por Anio y Region | Distribucion geografica de ventas |
| Participacion Electricos | % vehiculos alternativos vs tradicionales |
| Crecimiento YoY | Crecimiento porcentual anio a anio por region |
| Ventas por Combustible | Participacion de mercado por tipo de combustible |
| Ventas por Transmision | Automatico vs Manual |
| KPIs | Resumen ejecutivo con totales y crecimiento |

## Estructura del Proyecto

```
BMWdatawarehouse/
├── main.py              # Entry point - orquesta el pipeline
├── config.py            # Configuracion y rutas
├── utils.py             # Funciones auxiliares (logging)
├── etl/
│   ├── __init__.py      # Exporta funciones del pipeline
│   ├── extract.py       # Paso 1: Extraccion y validacion
│   ├── transform.py     # Paso 2-3: Dimensiones y tabla de hechos
│   └── views.py         # Paso 4: Vistas analiticas
├── bmw_sales.csv        # Dataset fuente (50,000 registros)
├── outputs/             # Archivos generados (CSVs)
└── temp/                # Archivos temporales
```

## Como Ejecutar

### Requisitos

- Python 3.8+
- pandas
- numpy

### Instalacion

```bash
pip install pandas numpy
```

### Ejecucion

```bash
python main.py
```

Los archivos generados se encuentran en la carpeta `outputs/`.

## Dataset

El dataset contiene 50,000 registros de ventas de BMW con las siguientes columnas:

| Columna | Descripcion |
|---------|-------------|
| Model | Modelo del vehiculo (3 Series, 5 Series, X1, X3, X5, M3, M5, i8, 7 Series) |
| Year | Anio de venta (2010-2024) |
| Region | Region geografica |
| Color | Color del vehiculo |
| Fuel_Type | Tipo de combustible |
| Transmission | Tipo de transmision |
| Engine_Size_L | Tamanio del motor en litros |
| Mileage_KM | Kilometraje |
| Price_USD | Precio en dolares |
| Sales_Volume | Volumen de ventas |
| Sales_Classification | Clasificacion (High/Low) |

## Metodologia

Se utilizo la **metodología HEFESTO** para el diseno del Data Warehouse, que propone un enfoque iterativo para la construccion de almacenes de datos partiendo del analisis de requerimientos hasta la implementacion del modelo dimensional.

## Tecnologias

- **Python** - Lenguaje principal
- **pandas** - Manipulacion y transformacion de datos
- **numpy** - Operaciones numericas
- **Power BI** - Visualizacion (destino de los datos)

## Autores

- Alfei Mateo
- Bono Nicolas
- Bocco Matias Juan
