from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, lit

@dp.table(
    name="commander_victories_gold",
    comment="A table that lists the victories by each commander along with the side they fought for."
)

def commander_victories_gold():
    battles_silver=spark.read.table("battles_silver") #Read in battles table 
    
    #Create Union Commander victory table
    union_victories=battles_silver.filter(col("result")=="Union Victory").withColumn("Side",lit("Union"))
    union_commander_victories=union_victories.groupBy("union_commander","Side").agg(count("battle_id").alias("Victories"))
    union_commander_victories=union_commander_victories.withColumnRenamed("union_commander","Commander")
    
    #Create Confederate Commander victory table
    confederate_victories=battles_silver.filter(col("result")=="Confederate Victory").withColumn("Side",lit("Confederate"))
    confederate_commander_victories=confederate_victories.groupBy("confederate_commander","Side").agg(count("battle_id").alias("Victories"))
    confederate_commander_victories=confederate_commander_victories.withColumnRenamed("confederate_commander","Commander")

    #Create combined victory table
    return(union_commander_victories.unionByName(confederate_commander_victories).orderBy(col("Victories").desc()))