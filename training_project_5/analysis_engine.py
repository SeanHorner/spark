from pyspark.sql import SparkSession
from databricks import koalas as ks
import numpy as np
import matplotlib as mpl

def main():
    # Setting up SparkSession for running the analysis
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("Crypto Coin Analysis") \
        .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.sparkContext.setLogLevel("ERROR")
