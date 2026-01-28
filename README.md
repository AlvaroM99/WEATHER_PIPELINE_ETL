# 🌦️ Weather ETL Pipeline

Pipeline ETL end-to-end para extraer datos meteorológicos de la API de OpenWeatherMap, transformarlos y cargarlos en una base de datos PostgreSQL, con orquestación mediante Apache Airflow y visualización con Metabase.

## 🏗️ Arquitectura del Proyecto

```
┌─────────────────┐
│ OpenWeatherMap  │
│      API        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Apache Airflow │ ────►│      MinIO      │
│   (Scheduler)   │      │    (Data Lake)  │
└────────┬────────┘      └────────┬────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐      ┌─────────────────┐
│   PostgreSQL    │◄─────│   Silver Layer  │
│    Database     │      │    (Parquet)    │
└────────┬────────┘      └─────────────────┘
         │
         ├──► pgAdmin (Gestión DB)
         │
         └──► Metabase (Visualización)
```

## 🛠️ Stack Tecnológico

- **Apache Airflow**: Orquestación y programación del pipeline ETL
- **MinIO**: Data Lake (S3 compatible) para capas Bronze y Silver
- **PostgreSQL**: Data Warehouse para almacenar datos finales (Gold Layer)
- **Docker Compose**: Infraestructura containerizada
- **Metabase**: Visualización y dashboards
- **pgAdmin**: Interfaz gráfica para gestión de PostgreSQL
- **OpenWeatherMap API**: Fuente de datos meteorológicos

## 📋 Requisitos Previos

- Docker Desktop instalado y funcionando
- Docker Compose v2.0+
- API Key de OpenWeatherMap (gratuita)
- Mínimo 8GB RAM disponible
- Mínimo 10GB espacio en disco

## 🚀 Inicio Rápido

### 1. Clonar o descargar el proyecto

```bash
cd WEATHER_PIPELINE_ETL
```

### 2. Configurar variables de entorno

El archivo `.env` ya está configurado con tu API key. Si necesitas modificarlo:

```bash
# Editar .env con tus credenciales
notepad .env
```

### 3. Iniciar todos los servicios

```bash
docker compose up --build
```

Este comando:
- Descarga todas las imágenes necesarias
- Crea la base de datos PostgreSQL
- Inicia Airflow (webserver + scheduler)
- Inicia pgAdmin
- Inicia Metabase

**Nota**: El primer inicio puede tardar 5-10 minutos mientras se descargan las imágenes y se inicializa Airflow.

### 4. Acceder a los servicios

Una vez que todos los contenedores estén ejecutándose:

| Servicio | URL | Usuario | Contraseña |
|----------|-----|---------|------------|
| **Airflow** | http://localhost:8080 | `admin` | `Airflow2024!` |
| **MinIO Console**| http://localhost:9001 | `minioadmin` | `MinIO2024!Secure` |
| **pgAdmin** | http://localhost:5050 | `admin@weather.com` | `PgAdmin2024!` |
| **Metabase** | http://localhost:3000 | *(configurar en primer acceso)* | - |

## 📊 Uso del Pipeline

### Ejecutar el DAG manualmente

1. Accede a Airflow: http://localhost:8080
2. Busca el DAG `weather_etl_pipeline`
3. Activa el toggle para habilitar el DAG
4. Haz clic en el botón "▶" (Trigger DAG) para ejecutarlo manualmente

### Programación automática

El DAG está configurado para ejecutarse automáticamente todos los días a las **6:00 AM**.

### Monitorear la ejecución

En Airflow puedes:
- Ver el estado de cada tarea (Extract → Transform → Load)
- Revisar los logs de cada tarea
- Ver el historial de ejecuciones

## 🗄️ Estructura de la Base de Datos

### Tabla: `weather`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | SERIAL | ID único (auto-incremental) |
| `city` | VARCHAR(100) | Nombre de la ciudad |
| `country` | VARCHAR(10) | Código del país |
| `latitude` | FLOAT | Latitud |
| `longitude` | FLOAT | Longitud |
| `temperature` | FLOAT | Temperatura (°C) |
| `feels_like` | FLOAT | Sensación térmica (°C) |
| `temp_min` | FLOAT | Temperatura mínima (°C) |
| `temp_max` | FLOAT | Temperatura máxima (°C) |
| `pressure` | INTEGER | Presión atmosférica (hPa) |
| `humidity` | INTEGER | Humedad (%) |
| `weather_main` | VARCHAR(50) | Condición principal (Rain, Clear, etc.) |
| `weather_description` | TEXT | Descripción detallada |
| `wind_speed` | FLOAT | Velocidad del viento (m/s) |
| `wind_deg` | INTEGER | Dirección del viento (grados) |
| `clouds` | INTEGER | Nubosidad (%) |
| `visibility` | INTEGER | Visibilidad (metros) |
| `date` | DATE | Fecha del registro |
| `timestamp` | TIMESTAMP | Marca de tiempo de inserción |

### Ciudades incluidas

- Madrid
- Barcelona
- Valencia
- Sevilla
- Bilbao
- Málaga
- Zaragoza

### Tabla: `lake_metadata`

Rastrea el linaje de datos desde el Data Lake hacia el DWH.

| Columna | Descripción |
|---------|-------------|
| `layer` | Capa de origen (silver/bronze) |
| `object_path` | Ruta del archivo en MinIO |
| `record_count` | Registros cargados |
| `status` | Estado de carga |

## 🔍 Consultar los Datos

### Usando pgAdmin

1. Accede a http://localhost:5050
2. Login con las credenciales configuradas
3. Clic derecho en "Servers" → "Register" → "Server"
4. Configurar conexión:
   - **Name**: Weather DB
   - **Host**: postgres
   - **Port**: 5432
   - **Database**: weatherdb
   - **Username**: weatheruser
   - **Password**: Weather2024!Secure

5. Ejecutar consultas SQL:

```sql
-- Ver todos los registros
SELECT * FROM weather ORDER BY date DESC, city;

-- Temperatura promedio por ciudad
SELECT city, AVG(temperature) as avg_temp
FROM weather
GROUP BY city
ORDER BY avg_temp DESC;

-- Datos del día actual
SELECT * FROM weather 
WHERE date = CURRENT_DATE
ORDER BY temperature DESC;
```

## 📈 Crear Dashboard en Metabase

1. Accede a http://localhost:3000
2. Completa la configuración inicial
3. Conecta a la base de datos PostgreSQL:
   - **Database type**: PostgreSQL
   - **Host**: postgres
   - **Port**: 5432
   - **Database name**: weatherdb
   - **Username**: weatheruser
   - **Password**: Weather2024!Secure

   *Nota: Metabase usa internamente una base de datos separada (`metabase_db`) para su propia configuración, por lo que tus datos de usuario de Metabase persistirán independientemente de los datos del clima.*

4. Crea visualizaciones:
   - Gráfico de líneas: Temperatura por ciudad a lo largo del tiempo
   - Gráfico de barras: Comparación de temperaturas entre ciudades
   - Tabla: Últimos registros meteorológicos

## 🛑 Detener los Servicios

```bash
# Detener todos los contenedores
docker compose down

# Detener y eliminar volúmenes (¡CUIDADO! Esto borra los datos)
docker compose down -v
```

## 🔧 Solución de Problemas

### Airflow no inicia

```bash
# Ver logs de Airflow
docker compose logs airflow

# Reiniciar solo Airflow
docker compose restart airflow
```

### Error de conexión a PostgreSQL

```bash
# Verificar que PostgreSQL esté ejecutándose
docker compose ps postgres

# Ver logs de PostgreSQL
docker compose logs postgres
```

### El DAG no aparece en Airflow

1. Verifica que el archivo `dags/weather_etl_dag.py` existe
2. Revisa los logs de Airflow para errores de sintaxis
3. Espera 1-2 minutos (Airflow escanea DAGs cada minuto)

### Error de API Key

Verifica que tu API key de OpenWeatherMap esté correctamente configurada en el archivo `.env`:

```bash
OPENWEATHER_API_KEY=78c48c7a080848cb36d4e2e6cd9f6170
```

## 📁 Estructura del Proyecto

```
WEATHER_PIPELINE_ETL/
├── dags/
│   └── weather_etl_dag.py      # DAG de Airflow con lógica ETL
├── docker/
│   ├── postgres/
│       ├── init.sql            # Script de inicialización de DB
│       ├── init-dimensional-tables.sql
│       └── init-fact-tables.sql
├── docker-compose.yml          # Configuración de servicios Docker
├── .env                        # Variables de entorno (NO subir a Git)
├── .env.example                # Template de variables de entorno
├── .gitignore                  # Archivos a ignorar en Git
└── README.md                   # Este archivo
```

## 🎯 Próximos Pasos

1. ✅ Ejecuta el DAG manualmente para verificar que funciona
2. ✅ Revisa los datos en pgAdmin
3. ✅ Crea tu primer dashboard en Metabase
4. 🚀 Personaliza las ciudades en `dags/weather_etl_dag.py`
5. 🚀 Ajusta la programación del DAG según tus necesidades
6. 🚀 Añade más visualizaciones en Metabase

## 📚 Recursos Adicionales

- [Documentación de Airflow](https://airflow.apache.org/docs/)
- [OpenWeatherMap API Docs](https://openweathermap.org/api)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Metabase User Guide](https://www.metabase.com/docs/latest/)

## 🤝 Contribuciones

Este proyecto es parte de un trabajo académico. Si encuentras algún problema o tienes sugerencias, no dudes en crear un issue.

## 📄 Licencia

Este proyecto es de código abierto y está disponible para fines educativos.

---

**¡Disfruta explorando los datos meteorológicos! 🌤️**
