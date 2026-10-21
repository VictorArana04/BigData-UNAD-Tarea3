# Tarea 3 – Procesamiento de Datos con Apache Spark

## Curso
**Big Data – UNAD**  
Código: **202016911** | Grupo: **72**

## Autor
**Víctor Manuel Jiménez Arana**

---

## Descripción General

Este repositorio contiene la solución práctica para la **Tarea 3 – Procesamiento de Datos con Apache Spark** del curso de Big Data.

La actividad se divide en dos componentes principales:
1. **Procesamiento Batch con Apache Spark (PySpark):** Análisis del histórico de la Tasa Representativa del Mercado (TRM) en Colombia.
2. **Procesamiento en Tiempo Real con Apache Kafka y Spark Streaming:** Implementación del flujo continuo de datos de eventos (Anexo 3).

---

# 1. Procesamiento Batch

## Dataset Seleccionado
- **Nombre:** Histórico Tasa Representativa del Mercado (TRM)
- **Fuente:** Datos Abiertos Colombia (`datos.gov.co`)
- **ID Dataset:** `32sa-8pi3`
- **Formato:** CSV
- **Registros:** +10.000 datos diarios de cotización del dólar en Pesos Colombianos (COP)
- **Rango de Fechas:** 1991 – 2026

## Tecnologías Utilizadas
- Apache Spark 3.5.9
- PySpark
- Python 3
- Matplotlib & Pandas (para visualizaciones)
- Ubuntu Server / VirtualBox

## Análisis Realizados
1. **Resumen Anual y Volatilidad:** Promedio anual de la TRM, valor máximo, mínimo y desviación estándar del dólar por año.
2. **Estacionalidad Mensual:** Promedio histórico de la TRM agrupado por mes (Enero a Diciembre).
3. **Picos Históricos:** Identificación del Top 10 de días con la TRM más alta registrada en Colombia (alcanzando el récord histórico de **$5.061,21 COP** en noviembre de 2022) y el Top 10 de días más bajos.

## Visualizaciones Generadas
El script de Batch genera automáticamente las siguientes gráficas en la carpeta `resultados/`:
- `grafico_01_trm_historico.png`: Evolución temporal de la TRM de 1991 a 2026.
- `grafico_02_promedio_anual.png`: Comportamiento del promedio anual de la TRM.
- `grafico_03_picos_trm.png`: Top 10 de picos máximos históricos del dólar.

---

# 2. Procesamiento en Tiempo Real (Streaming)

Para el componente de streaming se utilizó la infraestructura exigida en el **Anexo 3**:
- **Apache ZooKeeper:** Gestión de sesiones en puerto 2181.
- **Apache Kafka 3.6.2:** Broker de mensajería escuchando en el puerto 9092.
- **Tópico Kafka:** `sensor_data`

## Componentes
1. **`kafka_producer.py`:** Genera un flujo de datos en tiempo real (JSON) enviando eventos de prueba cada segundo (`sensor_id`, `temperature`, `humidity`, `timestamp`).
2. **`spark_streaming_consumer.py`:** Aplicación en PySpark que consume los datos de Kafka, agrupa por ventanas de tiempo de 1 minuto y calcula estadísticas en vivo.

---

# Estructura del Repositorio

```text
BigData-UNAD-Tarea3/
│
├── README.md
│
├── batch/
│   └── trm_batch_spark.py
│
├── streaming/
│   ├── kafka_producer.py
│   └── spark_streaming_consumer.py
│
└── resultados/
    ├── grafico_01_trm_historico.png
    ├── grafico_02_promedio_anual.png
    └── grafico_03_picos_trm.png
