from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, lit, year

@dp.table(
    name="annual_victories_gold",
    comment="A table that lists the victories by each side and year"
)

def annual_victories_gold():
    battles_silver=spark.read.table("workspace.civilwar2.battles_silver") #Read in battles table 
    
    battles_silver=battles_silver.withColumn("year",year(col("start_date")))
    #Create Union victory table
    union_victories=battles_silver.filter(col("result")=="Union Victory").withColumn("Side",lit("Union"))
    union_victories_by_year=union_victories.groupBy("year").agg(count("battle_id").alias("Union_Victories"))
    
    #Create Confederate Commander victory table
    confederate_victories=battles_silver.filter(col("result")=="Confederate Victory").withColumn("Side",lit("Confederate"))
    confederate_victories_by_year=confederate_victories.groupBy('year').agg(count("battle_id").alias("Confederate_Victories"))

    #Create combined victory table
    return(union_victories_by_year.join(confederate_victories_by_year,on="year",how="left").orderBy(col("year").asc()))