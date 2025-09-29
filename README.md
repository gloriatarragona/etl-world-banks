# Banks Project – ETL Pipeline for the World's Largest Banks

Este proyecto implementa un **pipeline ETL (Extract, Transform, Load)** en Python para adquirir, transformar y almacenar información sobre los **bancos más grandes del mundo**.  

## Descripción

El script `banks_project.py`:
1. **Extrae** datos desde una versión archivada de la página de Wikipedia sobre los bancos más grandes.
2. **Transforma** los datos, convirtiendo la capitalización de mercado en dólares a otras divisas (GBP, EUR, INR) usando un archivo CSV con tipos de cambio.
3. **Carga** los resultados en:
   - Un archivo CSV (`Largest_banks_data.csv`).
   - Una base de datos SQLite (`Banks.db`).
4. **Ejecuta consultas SQL** para obtener información agregada y ejemplos de resultados.

Además, se genera un archivo de log (`code_log.txt`) que registra el progreso de cada etapa del proceso.

## Requisitos

- Python 3.8+
- Librerías:
  - `requests`
  - `beautifulsoup4`
  - `pandas`
  - `numpy`
  - `sqlite3` (incluida en la librería estándar de Python)

Instalación de dependencias:
```bash
pip install requests beautifulsoup4 pandas numpy
```

## Uso

Ejecuta el script principal:

```bash
python banks_project.py
```

Esto generará:
- `Largest_banks_data.csv` → dataset procesado en formato CSV.  
- `Banks.db` → base de datos SQLite con la tabla `Largest_banks`.  
- `code_log.txt` → archivo de log con el progreso de la ejecución.  

## Consultas SQL incluidas

El script ejecuta automáticamente las siguientes consultas sobre la base de datos:
- Obtener todos los registros de la tabla `Largest_banks`.  
- Calcular la **media de la capitalización de mercado en GBP**.  
- Listar los **5 primeros bancos** de la tabla.  

Los resultados se muestran en la consola durante la ejecución.

## Estructura del proyecto

```
banks_project.py       # Script principal con el pipeline ETL
Largest_banks_data.csv # Salida en CSV (generado al ejecutar)
Banks.db               # Base de datos SQLite (generado al ejecutar)
code_log.txt           # Log de ejecución (generado al ejecutar)
```
