import sys
import json
import boto3
import time
import random
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Print boto3 version
print(f"boto3 version: {getattr(boto3, '__version__', 'unknown')}")

# =====================================================
# CONFIGURAÇÃO
# =====================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'execution_id',
    'bucket_name',
    'redshift_workgroup',
    'redshift_database'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = args['bucket_name']
EXECUTION_ID = args['execution_id']
RAW_PATH = f"s3://{BUCKET}/raw"
PROCESSED_PATH = f"s3://{BUCKET}/processed"
REDSHIFT_WORKGROUP = args['redshift_workgroup']
REDSHIFT_DATABASE = args['redshift_database']

bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
redshift_data = boto3.client('redshift-data', region_name='us-east-1')
s3_client = boto3.client('s3')

print("="*70)
print(f"OLIST PIPELINE COMPLETO - Execution ID: {EXECUTION_ID}")
print("="*70)

# =====================================================
# FASE 1: LEITURA DOS DADOS RAW
# =====================================================

print("\n[1/7] 📥 Lendo dados RAW...")

orders_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_orders_dataset.csv")

customers_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_customers_dataset.csv")

order_items_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_order_items_dataset.csv")

products_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_products_dataset.csv")

sellers_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_sellers_dataset.csv")

payments_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_order_payments_dataset.csv")

reviews_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_order_reviews_dataset.csv")

geo_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{RAW_PATH}/olist_geolocation_dataset.csv")

print(f"  ✓ Orders: {orders_df.count():,}")
print(f"  ✓ Customers: {customers_df.count():,}")
print(f"  ✓ Reviews: {reviews_df.count():,}")

# =====================================================
# FASE 2: TRANSFORMAÇÕES
# =====================================================

print("\n[2/7] 🧹 Aplicando transformações...")

orders_clean = orders_df \
    .dropDuplicates(['order_id']) \
    .filter(col('order_status').isNotNull()) \
    .withColumn('order_purchase_timestamp', to_timestamp('order_purchase_timestamp')) \
    .withColumn('order_approved_at', to_timestamp('order_approved_at')) \
    .withColumn('order_delivered_customer_date', to_timestamp('order_delivered_customer_date')) \
    .withColumn('order_estimated_delivery_date', to_timestamp('order_estimated_delivery_date')) \
    .withColumn('days_to_deliver', 
                datediff('order_delivered_customer_date', 'order_purchase_timestamp')) \
    .withColumn('delivery_delay_days',
                datediff('order_delivered_customer_date', 'order_estimated_delivery_date')) \
    .withColumn('is_delayed', 
                when(col('delivery_delay_days') > 0, lit(True)).otherwise(lit(False)))

payments_agg = payments_df \
    .groupBy('order_id') \
    .agg(
        sum('payment_value').alias('total_payment'),
        count('*').alias('payment_count'),
        max('payment_installments').alias('max_installments')
    )

items_agg = order_items_df \
    .groupBy('order_id') \
    .agg(
        count('*').alias('total_items'),
        sum('price').alias('total_price'),
        sum('freight_value').alias('total_freight')
    ) \
    .withColumn('total_order_value', col('total_price') + col('total_freight'))

# =====================================================
# FASE 3: FACT ORDERS
# =====================================================

print("\n[3/7] 🔗 Criando Fact Orders...")

fact_orders = orders_clean \
    .join(customers_df, 'customer_id', 'left') \
    .join(payments_agg, 'order_id', 'left') \
    .join(items_agg, 'order_id', 'left') \
    .select(
        'order_id',
        'customer_id',
        'customer_unique_id',
        'customer_zip_code_prefix',
        'customer_city',
        'customer_state',
        'order_status',
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
        'days_to_deliver',
        'delivery_delay_days',
        'is_delayed',
        coalesce('total_items', lit(0)).alias('total_items'),
        coalesce('total_price', lit(0.0)).alias('total_price'),
        coalesce('total_freight', lit(0.0)).alias('total_freight'),
        coalesce('total_order_value', lit(0.0)).alias('total_order_value'),
        coalesce('total_payment', lit(0.0)).alias('total_payment'),
        'payment_count',
        'max_installments'
    )

fact_orders_count = fact_orders.count()
print(f"  ✓ Fact Orders: {fact_orders_count:,}")

# Adicionar colunas de partição (filtrando nulos para evitar erro)
fact_orders_partitioned = fact_orders \
    .filter(col('order_purchase_timestamp').isNotNull()) \
    .withColumn('year', year('order_purchase_timestamp')) \
    .withColumn('month', month('order_purchase_timestamp'))

fact_orders_partitioned \
    .write \
    .mode('overwrite') \
    .partitionBy('year', 'month') \
    .parquet(f"{PROCESSED_PATH}/fact_orders/")

print(f"  ✓ Salvo: {PROCESSED_PATH}/fact_orders/")

# =====================================================
# FASE 4: ANÁLISE DE SENTIMENTO
# =====================================================

print("\n[4/7] 🤖 Análise de sentimento...")

reviews_with_text = reviews_df \
    .filter(col('review_comment_message').isNotNull()) \
    .filter(length(trim(col('review_comment_message'))) > 20) \
    .limit(200)

reviews_with_text_count = reviews_with_text.count()
print(f"  ℹ️  Reviews com texto: {reviews_with_text_count}")

if reviews_with_text_count == 0:
    print("  ⚠️  Pulando análise de sentimento")
    reviews_final = reviews_df.select(
        'review_id',
        'order_id',
        'review_score',
        lit(None).cast(StringType()).alias('review_comment_message'),
        lit(None).cast(StringType()).alias('ai_sentiment'),
        lit(None).cast(DoubleType()).alias('ai_sentiment_score'),
        lit(None).cast(StringType()).alias('ai_topics')
    )
else:
    def analyze_sentiment_bedrock(text):
        """Mock: Gera sentimento aleatório (Bedrock desabilitado temporariamente)"""
        if not text or len(text.strip()) < 20:
            return json.dumps({'sentiment': 'neutral', 'score': 0.5, 'topics': []})
        
        # Mock: Resultados aleatórios
        sentiments = ['positive', 'negative', 'neutral']
        topics_pool = ['entrega', 'qualidade', 'preço', 'atendimento', 'embalagem', 'produto', 'prazo']
        
        sentiment = random.choice(sentiments)
        score = random.uniform(0.3, 0.95) if sentiment != 'neutral' else random.uniform(0.4, 0.6)
        num_topics = random.randint(1, 3)
        topics = random.sample(topics_pool, num_topics)
        
        # Usar formato float direto (evitar conflito com pyspark.round)
        score_rounded = float(int(score * 100) / 100)
        
        return json.dumps({'sentiment': sentiment, 'score': score_rounded, 'topics': topics})
        
        # TODO: Habilitar Bedrock quando disponível
        # prompt = f"""Analise este review em português:
        # 
        # "{text[:500]}"
        # 
        # Responda APENAS com JSON (sem markdown):
        # {{"sentiment": "positive|negative|neutral", "score": 0.0, "topics": ["tópico1"]}}"""
        # 
        # try:
        #     response = bedrock.invoke_model(
        #         modelId='anthropic.claude-3-sonnet-20240229-v1:0',
        #         body=json.dumps({
        #             'anthropic_version': 'bedrock-2023-05-31',
        #             'max_tokens': 400,
        #             'messages': [{'role': 'user', 'content': prompt}],
        #             'temperature': 0.1
        #         })
        #     )
        #     
        #     result = json.loads(response['body'].read())
        #     content = result['content'][0]['text']
        #     
        #     import re
        #     json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
        #     return json_match.group(0) if json_match else content
        #     
        # except Exception as e:
        #     print(f"  ⚠️ Bedrock error: {str(e)}")
        #     return json.dumps({'sentiment': 'error', 'score': 0.0, 'topics': []})
    
    sentiment_udf = udf(analyze_sentiment_bedrock, StringType())
    
    reviews_enriched = reviews_with_text \
        .withColumn('sentiment_raw', sentiment_udf(col('review_comment_message'))) \
        .withColumn('sentiment_parsed', 
                    from_json('sentiment_raw', 
                             StructType([
                                 StructField('sentiment', StringType()),
                                 StructField('score', DoubleType()),
                                 StructField('topics', ArrayType(StringType()))
                             ]))) \
        .select(
            'review_id',
            'order_id',
            'review_score',
            'review_comment_message',
            col('sentiment_parsed.sentiment').alias('ai_sentiment'),
            col('sentiment_parsed.score').alias('ai_sentiment_score'),
            array_join(col('sentiment_parsed.topics'), ', ').alias('ai_topics')
        )
    
    reviews_without_text = reviews_df \
        .filter(col('review_comment_message').isNull() | 
                (length(trim(col('review_comment_message'))) <= 20)) \
        .select(
            'review_id',
            'order_id',
            'review_score',
            lit(None).cast(StringType()).alias('review_comment_message'),
            lit(None).cast(StringType()).alias('ai_sentiment'),
            lit(None).cast(DoubleType()).alias('ai_sentiment_score'),
            lit(None).cast(StringType()).alias('ai_topics')
        )
    
    reviews_final = reviews_enriched.union(reviews_without_text)
    
    print(f"  ✓ Análise concluída: {reviews_with_text_count} reviews")

reviews_final.write.mode('overwrite').parquet(f"{PROCESSED_PATH}/fact_reviews/")
print(f"  ✓ Salvo: {PROCESSED_PATH}/fact_reviews/")

# =====================================================
# FASE 5: DIMENSÕES
# =====================================================

print("\n[5/7] 📊 Criando dimensões...")

geo_avg = geo_df.groupBy('geolocation_zip_code_prefix') \
    .agg(
        avg('geolocation_lat').alias('latitude'),
        avg('geolocation_lng').alias('longitude')
    )

dim_customers = customers_df \
    .join(geo_avg, 
          customers_df.customer_zip_code_prefix == geo_avg.geolocation_zip_code_prefix,
          'left') \
    .select(
        'customer_id',
        'customer_unique_id',
        'customer_zip_code_prefix',
        'customer_city',
        'customer_state',
        'latitude',
        'longitude'
    ).distinct()

dim_customers.write.mode('overwrite').parquet(f"{PROCESSED_PATH}/dim_customers/")

dim_products = products_df.select(
    'product_id',
    'product_category_name',
    'product_weight_g',
    'product_length_cm',
    'product_height_cm',
    'product_width_cm'
).distinct()

dim_products.write.mode('overwrite').parquet(f"{PROCESSED_PATH}/dim_products/")

dim_sellers = sellers_df \
    .join(geo_avg,
          sellers_df.seller_zip_code_prefix == geo_avg.geolocation_zip_code_prefix,
          'left') \
    .select(
        'seller_id',
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state',
        'latitude',
        'longitude'
    ).distinct()

dim_sellers.write.mode('overwrite').parquet(f"{PROCESSED_PATH}/dim_sellers/")

print(f"  ✓ Dim Customers: {dim_customers.count():,}")
print(f"  ✓ Dim Products: {dim_products.count():,}")
print(f"  ✓ Dim Sellers: {dim_sellers.count():,}")

# =====================================================
# FASE 6: CARGA S3 (REDSHIFT DESABILITADO TEMPORARIAMENTE)
# =====================================================

print("\n[6/7] 📦 Validando dados salvos no S3...")

# Lista de tabelas processadas no S3
s3_tables = {
    'fact_orders': f"{PROCESSED_PATH}/fact_orders/",
    'fact_reviews': f"{PROCESSED_PATH}/fact_reviews/",
    'dim_customers': f"{PROCESSED_PATH}/dim_customers/",
    'dim_products': f"{PROCESSED_PATH}/dim_products/",
    'dim_sellers': f"{PROCESSED_PATH}/dim_sellers/"
}

s3_counts = {}
for table_name, s3_path in s3_tables.items():
    try:
        df = spark.read.parquet(s3_path)
        count = df.count()
        s3_counts[table_name] = count
        print(f"  ✓ {table_name}: {count:,} registros")
    except Exception as e:
        s3_counts[table_name] = 0
        print(f"  ⚠️ {table_name}: Erro ao ler - {str(e)}")

successful_loads = len([c for c in s3_counts.values() if c > 0])
failed_loads = [t for t, c in s3_counts.items() if c == 0]

print(f"\n  📊 Resultado S3: {successful_loads}/{len(s3_tables)} tabelas salvas com sucesso")

# TODO: Habilitar quando Redshift estiver configurado
# def execute_redshift_sql(sql, description):
#     """Executa SQL no Redshift via Data API"""
#     print(f"\n  🔄 {description}...")
#     
#     response = redshift_data.execute_statement(
#         WorkgroupName=REDSHIFT_WORKGROUP,
#         Database=REDSHIFT_DATABASE,
#         Sql=sql
#     )
#     
#     statement_id = response['Id']
#     
#     # Aguardar conclusão
#     max_attempts = 60
#     for attempt in range(max_attempts):
#         status_response = redshift_data.describe_statement(Id=statement_id)
#         status = status_response['Status']
#         
#         if status == 'FINISHED':
#             duration = status_response.get('Duration', 0) / 1_000_000_000
#             print(f"    ✅ Concluído em {duration:.2f}s")
#             return True
#         
#         elif status == 'FAILED':
#             error = status_response.get('Error', 'Unknown error')
#             print(f"    ❌ Falhou: {error}")
#             return False
#         
#         elif status in ['SUBMITTED', 'PICKED', 'STARTED']:
#             if attempt % 6 == 0:
#                 print(f"    ⏳ Em progresso... ({attempt * 10}s)")
#             time.sleep(10)
#     
#     print(f"    ⏰ Timeout")
#     return False
# 
# # Tabelas para carregar no Redshift
# tables_to_load = [
#     ('olist.fact_orders', f"s3://{BUCKET}/processed/fact_orders/"),
#     ('olist.fact_reviews', f"s3://{BUCKET}/processed/fact_reviews/"),
#     ('olist.dim_customers', f"s3://{BUCKET}/processed/dim_customers/"),
#     ('olist.dim_products', f"s3://{BUCKET}/processed/dim_products/"),
#     ('olist.dim_sellers', f"s3://{BUCKET}/processed/dim_sellers/")
# ]
# 
# for table_name, s3_path in tables_to_load:
#     sql = f"""
#     BEGIN TRANSACTION;
#     TRUNCATE TABLE {table_name};
#     COPY {table_name}
#     FROM '{s3_path}'
#     IAM_ROLE default
#     FORMAT AS PARQUET
#     ACCEPTINVCHARS;
#     COMMIT;
#     """
#     
#     success = execute_redshift_sql(sql, f"Carregando {table_name}")
#     
#     if success:
#         successful_loads += 1
#     else:
#         failed_loads.append(table_name)

# =====================================================
# FASE 7: VALIDAÇÃO E MÉTRICAS
# =====================================================

print("\n[7/7] 📈 Validação final...")

print("\n  📊 Contagens finais no S3:")
for table_name, count in s3_counts.items():
    print(f"    {table_name}: {count:,}")

# TODO: Habilitar validação Redshift quando disponível
# validation_sql = """
# SELECT 
#     'fact_orders' as table_name, COUNT(*) as count FROM olist.fact_orders
# UNION ALL
# SELECT 'fact_reviews', COUNT(*) FROM olist.fact_reviews
# UNION ALL
# SELECT 'dim_customers', COUNT(*) FROM olist.dim_customers
# UNION ALL
# SELECT 'dim_products', COUNT(*) FROM olist.dim_products
# UNION ALL
# SELECT 'dim_sellers', COUNT(*) FROM olist.dim_sellers;
# """
# 
# response = redshift_data.execute_statement(
#     WorkgroupName=REDSHIFT_WORKGROUP,
#     Database=REDSHIFT_DATABASE,
#     Sql=validation_sql
# )
# 
# time.sleep(5)
# 
# try:
#     result = redshift_data.get_statement_result(Id=response['Id'])
#     
#     print("\n  Contagens Redshift:")
#     for record in result['Records']:
#         table_name = record[0]['stringValue']
#         count = record[1]['longValue']
#         print(f"    {table_name}: {count:,}")
# except Exception as e:
#     print(f"  ⚠️ Erro ao validar: {str(e)}")

# Estatísticas finais
stats = {
    'execution_id': EXECUTION_ID,
    'execution_timestamp': datetime.now().isoformat(),
    'storage': 'S3',  # Indica onde os dados estão armazenados
    'redshift_enabled': False,
    's3_path': PROCESSED_PATH,
    'tables': {
        'fact_orders': s3_counts.get('fact_orders', 0),
        'fact_reviews': s3_counts.get('fact_reviews', 0),
        'dim_customers': s3_counts.get('dim_customers', 0),
        'dim_products': s3_counts.get('dim_products', 0),
        'dim_sellers': s3_counts.get('dim_sellers', 0)
    },
    'total_orders': fact_orders_count,
    'total_customers': s3_counts.get('dim_customers', 0),
    'total_products': s3_counts.get('dim_products', 0),
    'total_sellers': s3_counts.get('dim_sellers', 0),
    'total_reviews': reviews_df.count(),
    'reviews_with_sentiment': reviews_with_text_count,
    's3_loads_successful': successful_loads,
    's3_loads_failed': len(failed_loads)
}

# Salvar métricas
s3_client.put_object(
    Bucket=BUCKET,
    Key=f'processed/metrics/{EXECUTION_ID}.json',
    Body=json.dumps(stats, indent=2)
)

print("\n" + "="*70)
print("✅ PIPELINE COMPLETO FINALIZADO COM SUCESSO!")
print("="*70)
for key, value in stats.items():
    print(f"  {key}: {value}")
print("="*70)

job.commit()