from datetime import datetime


def log(mensaje, tipo="INFO"):
    """Registra mensajes con timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    prefijo = {
        'INFO': '📋',
        'SUCCESS': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'PROCESS': '⚙️'
    }.get(tipo, '📋')
    print(f"[{timestamp}] {prefijo} {mensaje}")


def print_header(titulo):
    """Imprime un encabezado formateado"""
    print("\n" + "═" * 80)
    print(f"  {titulo}")
    print("═" * 80 + "\n")
