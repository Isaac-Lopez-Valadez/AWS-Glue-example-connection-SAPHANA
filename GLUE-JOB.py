import sys
import vertica_python #Import vertica librery from details Job 
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, withColumn, select
from datetime import datetime, timedelta
import calendar

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Star ETL
vertica_jdbc_url = "URL.from.DATABASE"
conn_db = {
    'host': '10.0.0.1',
    'port': 5433,
    'user': 'user_root',
    'password': 'passw0rd_root',
    'database': 'subscriptions'
}

        
try:
    
    status_query = """
        SELECT * FROM nameDB.tableName WHERE status = 1 AND subscriptionsStatus = 3
    """

    #conneccion por spark read (EXTRACTION)
    vertica_df = spark.read \
        .format("jdbc") \
        .option("url", vertica_jdbc_url) \
        .option("dbtable", f"({status_query}) as temp") \
        .option("user", conn_db['user']) \
        .option("password", conn_db['password']) \
        .option("driver", "com.vertica.jdbc.Driver") \
        .load()
    
    count = vertica_df.count()
    
    if count == 0:
        print("no se encontraron registros")
    else:
        
        #(TRANSFORM)
        for row in vertica_df.collect():
            subscription_id = row['subscription']
            description = row['description']
            star_date = row['starDate']
            end_date = row['endDate']
            
            sap_query = """
                SELECT * FROM SAPABAP.SUBS
            """
            #coneccion a sap utilizando el connection de glue
            SAPHANA_node = glueContext.create_dynamic_frame.from_options(
            connection_type="saphana",
            connection_options={
                "connectionName": "Conexion SAP",
                "query": sap_query,
            },
            transformation_ctx="SAPHANA_node"
            )
            #Convertir a DataFrame para manipular los datos
            SAPHANA_df = SAPHANA_node.toDF()
            
            #Convertir columnas
            SAPHANA_df = SAPHANA_df.withColumn("ColumnNameStartDate", to_date(col("ColumnNameStartDate"), "yyyyMMdd")) \
                                   .withColumn("ColumnNameEndDate", to_date(col("ColumnNameEndDate"), "yyyyMMdd"))
            
            
            #Modificar nombre de las columas a columnas de la BD
            SAPHANA_df = SAPHANA_df.select(
                col("columnName").alias("name"),
                col("columnSubscription").alias("subscription"),
                col("columnContract").alias("contract"),
                col("columnStartDate").alias("statDate"),
                col("columnEndDate").alias("endDate")
            )
            
            #Conecion a vertica y escribir los datos tranformados(Load)
            SAPHANA_df.write \
                .format("jdbc") \
                .option("url", vertica_jdbc_url) \
                .option("dbtable", "db.TableDBSubscriptions") \
                .option("user", conn_db['user']) \
                .option("password", conn_db['password']) \
                .option("driver", "com.vertica.jdbc.Driver") \
                .mode("append") \
                .save()


finally:
    #End ETL
    job.commit()