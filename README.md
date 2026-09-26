# E-commerce Data Pipeline — Web Scraping to AWS S3
 
Pipeline de datos end-to-end que simula un proyecto real de ingeniería de datos: scraping de múltiples sitios de e-commerce, ingesta a un data lake en S3, transformación con PySpark, validación de calidad de datos, catalogación con Glue/Athena, y orquestación diaria con Airflow.
 
## Arquitectura
 
```
                    ┌─────────────────┐
                    │  Apache Airflow │  (orquestación diaria)
                    └────────┬────────┘
                             │
        ┌────────────────────┴────────────────────┐
        ▼                                          ▼
┌───────────────┐                         ┌───────────────────┐
│  Scrapy        │                         │  Scrapy            │
│  webscraper_cars                         │  webscraper_ecommerce
└───────┬───────┘                         └─────────┬─────────┘
        ▼                                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      S3 — RAW (Bronze)                        │
│  raw/text/{sitio}/{fecha}/products.json                       │
│  raw/images/{sitio}/{fecha}/*.jpg                              │
│  raw/manifests/{sitio}/{fecha}/manifest.json                  │
└──────────────────────────────┬────────────────────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │   PySpark ETL          │
                    │   (tipado, dedup,      │
                    │   Data Quality)        │
                    └───────────┬───────────┘
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    S3 — CURATED (Silver)                      │
│  curated/text/{sitio}/{fecha}/*.parquet      (válidos)         │
│  curated/rejected/{sitio}/{fecha}/*.parquet  (rechazados)      │
└──────────────────────────────┬────────────────────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │  AWS Glue Crawler      │
                    │  (1 por sitio)         │
                    └───────────┬───────────┘
                                ▼
                    ┌───────────────────────┐
                    │  Amazon Athena         │
                    │  (consulta SQL)        │
                    └───────────┬───────────┘
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                       S3 — GOLD                                │
│  gold/cars_availability_summary/                                │
│  gold/ecommerce_category_summary/                                │
│  (tablas agregadas, listas para consumo de negocio)             │
└─────────────────────────────────────────────────────────────┘
 
        Notificaciones de resultado → Slack (webhook)
```
 
## Fuentes de datos
 
Dos sitios de práctica de web scraping (legales, sin restricciones de `robots.txt` para las rutas usadas), elegidos para demostrar que el diseño escala a múltiples dominios de e-commerce con esquemas distintos:
 
| Sitio | Spider | Categoría | Registros |
|---|---|---|---|
| [webscraper.io/test-sites/pagination](https://webscraper.io/test-sites/pagination) | `webscraper_cars` | Autos clásicos | ~150 |
| [webscraper.io/test-sites/e-commerce/allinone](https://webscraper.io/test-sites/e-commerce/allinone) | `webscraper_ecommerce` | Laptops, tablets, celulares | ~150 |
 
## Stack tecnológico
 
| Capa | Tecnología |
|---|---|
| Orquestación | Apache Airflow (Astro Runtime) |
| Ingesta | Python + Scrapy |
| Data Lake | Amazon S3 |
| Transformación | PySpark |
| Formato de datos curados | Parquet |
| Catálogo | AWS Glue Data Catalog |
| Consulta | Amazon Athena |
| Contenedores | Docker |
| Notificaciones | Slack (Incoming Webhooks) |
| Calidad de datos | Reglas custom en PySpark (validación + cuarentena) |
 
## Estructura del repositorio
 
```
E-commerce Data Pipeline/
├── scraper/                       # Proyecto Scrapy
│   ├── scraper/
│   │   ├── items.py                # CarItem, ProductItem
│   │   ├── pipelines.py            # ImagesPipeline -> S3
│   │   ├── settings.py             # Config S3, FEEDS
│   │   └── spiders/
│   │       ├── webscraper_cars.py
│   │       └── webscraper_ecommerce.py
│   └── scrapy.cfg
├── etl/
│   ├── raw_to_curated.py            # ETL de autos (RAW -> Silver + DQ)
│   └── raw_to_curated_ecommerce.py  # ETL de e-commerce
├── docker/
│   ├── Dockerfile.scraper
│   └── Dockerfile.etl
├── airflow/                        # Proyecto Astro (Airflow)
│   ├── dags/
│   │   └── ecommerce_pipeline_dag.py
│   ├── include/                    # Copia del código (scraper/ + etl/)
│   ├── Dockerfile                  # Astro Runtime + Java (para PySpark)
│   └── requirements.txt
├── requirements.txt
├── .env.example
└── README.md
```
 
## Flujo de datos (medallion architecture)
 
1. **Bronze (`raw/`)** — JSON crudo tal cual llega del scraping + imágenes + manifest de la corrida. Nunca se transforma, solo se ingesta.
2. **Silver (`curated/`)** — PySpark tipa columnas, deduplica por `sku` (se queda con el registro más reciente por `scraped_at`), y aplica reglas de Data Quality. Los registros que fallan DQ van a `curated/rejected/` en vez de perderse silenciosamente.
3. **Gold (`gold/`)** — Tablas agregadas para consumo directo de negocio (ej. resumen de disponibilidad de autos, resumen de precios por categoría de e-commerce), creadas con `CREATE TABLE AS SELECT` en Athena.
## Reglas de Data Quality
 
Cada registro se valida contra reglas específicas de su dominio (ej. para autos: `sku` no vacío, precio > 0, año en rango válido, kilometraje no negativo, consistencia entre estado y unidades disponibles). Los errores se acumulan en una columna `dq_errors`; si un registro tiene al menos un error, se enruta a la carpeta de rechazados en vez del dataset final.
 
El pipeline además calcula la **tasa de rechazo** de cada corrida: si supera el 15%, el ETL falla intencionalmente (en vez de subir silenciosamente un dataset con más de 1 de cada 7 registros dañados), lo cual en Airflow dispara la notificación de fallo.
 
## Orquestación (Airflow)
 
El DAG `ecommerce_scraping_pipeline` corre diariamente (`@daily`) con el siguiente flujo:
 
```
run_scraper_cars ──> verify_raw_files_cars ──> run_etl_cars ──┐
                                                                ├──> notify_success / notify_failure
run_scraper_ecommerce ──> verify_raw_files_ecommerce ──> run_etl_ecommerce ──┘
```
 
- Ambos sitios se scrapean **en paralelo**, sin dependencia entre sí.
- `verify_raw_files_*` confirma que el scraping generó datos antes de intentar transformarlos.
- La notificación final reporta el estado de **cada** ETL por separado (vía XCom), y usa `trigger_rule` para dispararse solo si todo salió bien (`notify_success`) o si algo falló (`notify_failure`).
## Cómo correrlo
 
### Requisitos
- Python 3.11+, Java 17+ (para PySpark), Docker Desktop, Astro CLI
- Una cuenta de AWS con un bucket S3 y credenciales IAM con permisos mínimos (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`)
- (Opcional) Un Incoming Webhook de Slack para notificaciones
### Configuración
1. Copia `.env.example` a `.env` y completa tus credenciales de AWS y bucket.
2. Instala dependencias: `pip install -r requirements.txt`
### Correr el scraping localmente
```bash
cd scraper
scrapy crawl webscraper_cars
scrapy crawl webscraper_ecommerce
```
 
### Correr el ETL localmente
```bash
python etl/raw_to_curated.py
python etl/raw_to_curated_ecommerce.py
```
 
### Correr con Docker
```bash
docker build -f docker/Dockerfile.scraper -t ecommerce-scraper .
docker build -f docker/Dockerfile.etl -t ecommerce-etl .
 
docker run --rm --env-file .env ecommerce-scraper webscraper_cars
docker run --rm --env-file .env ecommerce-etl
```
 
### Correr con Airflow (Astro)
```bash
cd airflow
astro dev start
```
Abre `http://localhost:8080`, dispara el DAG `ecommerce_scraping_pipeline`.
 
### Configurar AWS Glue + Athena
1. Crea una base de datos en Glue por cada sitio (o una compartida, ajustando el nombre en las queries).
2. Crea un Crawler por sitio, apuntando a `s3://<bucket>/curated/text/<sitio>/`.
3. Corre el Crawler para catalogar el esquema y las particiones.
4. Consulta desde Athena, y crea las tablas Gold con `CREATE TABLE ... AS SELECT`.
## Decisiones de diseño y aprendizajes
 
- **Ingesta local → S3, no Spark con `s3a` directo**: en Windows, configurar Hadoop/`s3a` para que PySpark lea/escriba directo en S3 es frágil. Se optó por boto3 para descargar el RAW a disco temporal, procesarlo local con PySpark, y subir el resultado — más simple y confiable para este entorno.
- **Estructura de particiones `{sitio}/{fecha}/`** en vez de `{fecha}/{sitio}/`: al tener dos fuentes con esquemas distintos, cada una necesita su propia tabla en Glue — el sitio como partición raíz facilita aislar, aplicar permisos y reprocesar por fuente sin tocar la otra.
- **Cuarentena de datos rechazados** en vez de descartarlos: mantiene trazabilidad completa de qué falló y por qué (columna `dq_errors`), permitiendo auditoría y reprocesamiento posterior.
 
