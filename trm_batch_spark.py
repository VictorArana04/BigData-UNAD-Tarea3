from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, avg, max, min, count, round, desc

def main():
    # 1. Inicializar Spark
    spark = SparkSession.builder \
        .appName("Analisis_TRM_Colombia_Batch") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("\n" + "="*80)
    print(" INICIANDO PROCESAMIENTO EN BATCH - ANÁLISIS HISTÓRICO TRM ")
    print("="*80 + "\n")

    # 2. Cargar el dataset CSV
    ruta_dataset = "trm_colombia.csv"
    df_raw = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(ruta_dataset)

    print("--- Esquema del Dataset Original ---")
    df_raw.printSchema()

    # 3. Limpieza y Transformación
    # Normalización de columnas de datos.gov.co (VALOR y VIGENCIADESDE)
    col_valor = "VALOR" if "VALOR" in df_raw.columns else df_raw.columns[0]
    col_fecha = "VIGENCIADESDE" if "VIGENCIADESDE" in df_raw.columns else df_raw.columns[1]

    df_clean = df_raw \
        .withColumn("valor_trm", col(col_valor).cast("double")) \
        .withColumn("fecha", to_date(col(col_fecha), "dd/MM/yyyy")) \
        .filter(col("valor_trm").isNotNull())

    # Si el formato de fecha no parseó con dd/MM/yyyy, intentar yyyy-MM-dd
    if df_clean.filter(col("fecha").isNotNull()).count() == 0:
        df_clean = df_raw \
            .withColumn("valor_trm", col(col_valor).cast("double")) \
            .withColumn("fecha", to_date(col(col_fecha), "yyyy-MM-dd")) \
            .filter(col("valor_trm").isNotNull())

    df_transformed = df_clean \
        .withColumn("anio", year(col("fecha"))) \
        .withColumn("mes", month(col("fecha")))

    print("--- Muestra de Datos Limpios y Transformados ---")
    df_transformed.select("fecha", "anio", "mes", "valor_trm").show(10, truncate=False)

    # 4. Agregaciones / Análisis Exploratorio
    print("--- Resumen Anual de la TRM (Promedio, Máx, Mín) ---")
    df_anual = df_transformed.groupBy("anio") \
        .agg(
            round(avg("valor_trm"), 2).alias("promedio_trm"),
            round(max("valor_trm"), 2).alias("maximo_trm"),
            round(min("valor_trm"), 2).alias("minimo_trm"),
            count("valor_trm").alias("total_registros")
        ) \
        .sort(desc("anio"))

    df_anual.show(20, truncate=False)

    print("--- Top 10 Picos Más Altos de la TRM en Colombia ---")
    df_picos = df_transformed.select("fecha", "valor_trm") \
        .sort(desc("valor_trm")) \
        .limit(10)

    df_picos.show(truncate=False)

    print("="*80)
    print(" PROCESAMIENTO EN BATCH COMPLETADO CON ÉXITO ")
    print("="*80 + "\n")
    spark.stop()

if __name__ == "__main__":
    main()
