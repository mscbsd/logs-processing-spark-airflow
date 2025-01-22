from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

def processar_logs(bucket_name, date):
    """Processa logs CSV do S3, gera relatórios e salva em Parquet."""

    spark = SparkSession.builder.appName("ProcessamentoLogs").getOrCreate()

    try:
        # Configuração para acesso ao S3 (usando variáveis de ambiente)
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true") # Para usar path-style access (necessário em algumas configurações)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")

        # Leitura dos logs do S3 (substitua pelo seu padrão de nome de arquivo)
        log_path = f"s3a://{bucket_name}/logs/log_{date}*.csv" # Ex: logs/log_20240727*.csv
        df = spark.read.csv(log_path, header=True, inferSchema=True)

        if df.count() == 0:
            print(f"Nenhum log encontrado para a data: {date}")
            return

        # Conversão do timestamp para formato adequado
        df = df.withColumn("timestamp", to_timestamp("timestamp"))

        # Contagem de acessos por hora
        acessos_por_hora = df.groupBy(hour("timestamp").alias("hora")).count().orderBy("hora")

        # Contagem de acessos únicos por IP
        acessos_unicos_por_ip = df.groupBy("ip").count().orderBy(desc("count"))

        # Detecção de possíveis acessos maliciosos
        acessos_maliciosos = df.groupBy(hour("timestamp").alias("hora"), "ip").count().filter("count > 1000").orderBy(desc("count"))

        # Salvar relatórios em formato Parquet no S3
        output_path = f"s3a://{bucket_name}/relatorios/data={date}/"
        acessos_por_hora.write.mode("overwrite").parquet(output_path + "acessos_por_hora")
        acessos_unicos_por_ip.write.mode("overwrite").parquet(output_path + "acessos_unicos_por_ip")
        acessos_maliciosos.write.mode("overwrite").parquet(output_path + "acessos_maliciosos")

        print(f"Relatórios gerados e salvos em: {output_path}")

    except Exception as e:
        print(f"Erro no processamento: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Uso: python processar_logs.py <bucket_name> <data (AAAAMMDD)>")
        sys.exit(1)

    bucket_name = sys.argv[1]
    date = sys.argv[2]
    processar_logs(bucket_name, date)