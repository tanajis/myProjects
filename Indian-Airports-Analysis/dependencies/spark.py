import __main__
from pyspark.sql import SparkSession
from pyspark import SparkFiles



def getSparkSession(master = 'local[*]',appName = 'mySpark',spark_config={},jarFiles = [],files = []):

    """
    This function return spark Session object.
    It takes belo parameters.
    param master  : Cluster connection. Defalt is local[*]
    param appName : The name of application
    param jarFiles : List of Spark Jar files
    param files : List of other files like dependencies, packages, configs etc.They
                  are sent to all the nodes as Spark files.
    param spark_config : Dictionary of config key-value pairs.
    return : return spark session object
    """
    
    #Create builder Object
    spark_builder = (
        SparkSession
        .builder
        .master(master)
        .appName(appName))

    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jarFiles))
    #Add jar files
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    #Add other files
    spark_files = ','.join(files)
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    #Create Spark Session now
    spark = spark_builder.getOrCreate()

    spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    return spark
