#!/usr/bin/env python
#=============================================================================================
# Title           :heap_array.py
# Description     :This module contains array implementation of Heap data strcuture.
# Author          :Tanaji Sutar
# Date            :2020-Mar-30
# python_version  :2.7/3
#============================================================================================


import math
from pyspark.sql import DataFrame
from dependencies.spark import getSparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct
from pyspark.sql.functions import *


def selectRequiredColumns(df,requiredColumns):
    """
    """
    pass 
    #id,name,ident,type,municipality,score,latitude_deg,longitude_deg,elevation_ft,iso_region

def getDistanceUsingLatLongs(lat1,lon1,lat2,lon2):
    """
    This function return distinace between to latitude and longitudes.
    """
    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)

    radius = 6371 # km
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
    * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    d = int(radius * c)
    
    return d


#define udf pass row object
#getDistanceUsingLatLongs  = UserDefinedFunction(lambda x1,y1,x2,y2: getDistanceUsingLatLongs(x1,y1,x2,y2),FloatType())


def applyDQ():
    #make columns upper
    #remove Null records
    pass

def getSateWiseMeanScore(inputDF:DataFrame):
    """
    This function returns dataframe containting iso_regions and its mean score.
    param inputDF: pyspark dataframe
    """
    newdf  = inputDF.filter("type != 'closed'")
    newdf  = newdf.select('iso_region','score')
    newdf.registerTempTable('temp_table')
    newdf = spark.sql("select iso_region,mean(score) as mean_score from temp_table group by  iso_region ")
    
    return newdf


def getStateWiseHighScoreAirport():
    """
    this function return state wise high score Airports.
    With difference between its score and mean score of country.
    """
    pass


def getAllAiportDistnace(inputDF:DataFrame):
    """
    This function retruns dataframe with Airports with its distance from all other Airports.
    """
    newdf = inputDF.select(['id','name','type','latitude_deg','longitude_deg'])
    newdf.registerTempTable('temp_table')

    #get distance from every other airport
    qry = """
    select 
    a.id,a.name,
    b.id as other_id,
    b.name as other_name,
    b.type as other_type,
    getDistanceUsingLatLongs(a.latitude_deg,a.longitude_deg,b.latitude_deg,b.longitude_deg) as distance
    from temp_table a 
    cross join 
    temp_table b
    where
    a.id !=b.id
    """
    newdf = spark.sql(qry)
    return newdf



def getNearestAirport(inputDF:DataFrame):
    """
    This function returns nearest Airport
    """

    #get distance from every other airport
    df_allAirportDist = getAllAiportDistnace(inputDF)

    #filter : nearest Should be less than 1000 km
    newdf = df_allAirportDist.filter("distance <= 1000 ")

    #filter : other should be either large_airport or medium_airport
    newdf = newdf.filter("other_type ='large_airport' or other_type ='medium_airport'")

    #Selectiong nearest:
    #if nearest is largets : ok
    #elif nearest is medium_airport:
    #   Check if any large_airport exist within extra 200 km distance(medium_distnace + 200):
    #       if exist :take that large_airport as nearest
    #       else: take medium_airport as nearest

    #get min distnace with medium_airport aiport
    
    df_med_min_dist = newdf.filter("other_type == 'medium_airport'")\
        .groupBy('id','name').min('distance')
    
    df_med_min_dist = df_med_min_dist.select('id','name',df_med_min_dist['min(distance)'].alias('min_dist_medium'))
    
    #get id and name of medium airport
    tempdf = newdf.filter("other_type == 'medium_airport'")
    df_med_min_dist = df_med_min_dist.join(tempdf,(df_med_min_dist.id == tempdf.id) & (df_med_min_dist.min_dist_medium == tempdf.distance), 'inner')


    df_large_min_dist = newdf.filter("other_type == 'large_airport'")\
        .groupBy('id','name').min('distance')
    
    df_large_min_dist = df_large_min_dist.select('id','name',df_large_min_dist['min(distance)'].alias('min_dist_large'))
    
    #get id and name of min airport
    tempdf = newdf.filter("other_type == 'large_airport'")
    df_large_min_dist = df_large_min_dist.alias('a').join(tempdf.alias('b'),(df_large_min_dist.id == tempdf.id) & (df_large_min_dist.min_dist_large == tempdf.distance) ,'inner')

    df_large_min_dist.printSchema()
    #if nearest is larger then keep as it is
    final_large_df = df_large_min_dist.select(['b.id','b.name','b.other_id','a.other_name','a.other_type','b.distance'])

    #filter remaining
    newdf.registerTempTable('newdf')
    final_large_df.registerTempTable('final_large_df')
    df_med_min_dist.registerTempTable('df_med_min_dist')
    
    #take medium for those who dont have larger nearest airport.take medium for them
    df_with_no_large_type_min = spark.sql(
        """
        with abc as (
        select a.id,a.name 
                from newdf a 
                left outer join 
                final_large_df b
                where b.id is NULL)
        select 
            a.id,a.name,
            a.other_id,
            a.other_type,
            a.distance,
            from abc  a
            innert join
            df_med_min_dist b
            on 
            a.id == b.id and a.distance ==b.distance
        """)

    


    #if nearest is mediam then check if it has large within
    return df_with_no_large_type_min



    """
    dist_df = spark.sql(qry)


    #projection_column = ['id','name','ident','type','municipality','score','latitude_deg','longitude_deg','elevation_ft','iso_region']
    #check distance with every other airport
    #if difference between nearest small and nearest large airport is less than 50km
    #then prefer large airport
    #find nearest1 ,nearest2 and nearest 3 with their distance
    """
    return dist_df

def main():
    #get Spark Session Object
    global spark
    spark = getSparkSession(master='local',appName='Indian Airlines',spark_config={},jarFiles=[],files=[])
    
    #Define user defined function as udf
    udf_getDist = udf(getDistanceUsingLatLongs,IntegerType())
    
    #Register that udf in spark
    _ = spark.udf.register('getDistanceUsingLatLongs',udf_getDist)
    
    
    #read from csv
    SRC_FILE_PATH = ".//data//sample-data.csv"
    source_df = spark.read.csv(path = SRC_FILE_PATH,header = True,sep = ',')

    source_df = source_df.limit(5)
    #Data Quality
    source_df = source_df.filter(" id != '#meta +id'")

    #project only required columns
    projection_column = ['id','name','ident','type','municipality','score','latitude_deg','longitude_deg','elevation_ft','iso_region']
    
    source_df = source_df.select(projection_column)
    #source_df.show(5)

    #
    #print('distance:',getDistanceUsingLatLongs(19.0760,72.8777,18.5204,73.8567))

    #Get State wise mean score
    #df_state_core_mean = getSateWiseMeanScore(source_df)
    #df_state_core_mean.show()


    #Get nearest Airport
    df_nearest_airport = getNearestAirport(source_df)
    df_nearest_airport.show()

if __name__ == "__main__":
    main()
    
