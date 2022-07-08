from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier
from pyspark.ml.classification import RandomForestClassifier,  LogisticRegression , NaiveBayes, MultilayerPerceptronClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, IndexToString, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

kafka_topic_name = "demo15"
kafka_bootstrap_servers = 'localhost:9092'
persistedModel = PipelineModel.load("C:\kafka-demo\model")
cluster = Cluster()

session = cluster.connect('k1')

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

        
    orders_schema_string = '''ID STRING ,QUARTER INT, MONTH INt, DAY_OF_MONTH INT, DAY_OF_WEEK INT ,
            OP_UNIQUE_CARRIER STRING,
            ORIGIN STRING,
            DEST STRING, DISTANCE DOUBLE,
            CRS_DEP_TIME DOUBLE,
            OUTPUT DOUBLE'''

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    
    
    orders_df3.printSchema()

    # Simple aggregate - find total_order_amount by grouping country, city
    # orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
    #     .agg({'order_amount': 'sum'}) \
    #     .select("order_country_name", "order_city_name", col("sum(order_amount)") \
    #     .alias("total_order_amount"))

    # print("Printing Schema of orders_df4: ")
    # orders_df4.printSchema()

    
    

    

    
    prediction1 = persistedModel.transform(orders_df3)
    predicted1 = prediction1.select('OUTPUT', "prediction",'timestamp')
    predicted2 = prediction1.select('ID' ,'QUARTER' , 'DAY_OF_MONTH' , 'DAY_OF_WEEK' , 'FL_DATE',
            'OP_UNIQUE_CARRIER_CATE', 'OP_CARRIER_FL_NUM_NOR',
            'ORIGIN_CATE',
            'DEST_CATE',
            'CRS_DEP_HOUR', 'DISTANCE_NOR',
            'OUTPUT', "prediction")
    
    
    orders_agg_write_stream1 = predicted2 \
        .writeStream \
        .trigger(processingTime = "5 seconds")\
        .outputMode("append") \
        .option("path", "C:/kafka-demo/output/")\
        .option("checkpointLocation", "/user/kafka_stream_test_out/chk") \
        .format("csv") \
        .start()
    orders_agg_write_stream = predicted1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    # orders_agg_write_stream2 = predicted1 \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .options(table="test", keyspace="k1")\
    #     .format("org.apache.spark.sql.cassandra") \
    #     .start()
        
    # prediction1.toPandas()
    
    

    orders_agg_write_stream1.awaitTermination()  
    orders_agg_write_stream.awaitTermination()
    # orders_agg_write_stream2.awaitTermination()
    print("Stream Data Processing Application Completed.")