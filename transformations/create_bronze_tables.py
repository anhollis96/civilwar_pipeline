from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@dp.table(
    name="battles_bronze",
    comment="Create materialized view from battle dataset"
)
@dp.expect("check_valid_dates","start_date>='1861-04-12' AND end_date<='1865-04-09'")
@dp.expect("valid_state_inidicator" , "state RLIKE '^[A-Z]{2}$'")
@dp.expect("valid_casualties",
           "CASE WHEN total_casualties IS NOT NULL AND total_casualties BETWEEN 0 AND 60000 then TRUE WHEN total_casualties IS NULL THEN TRUE ELSE FALSE END")
def battles_bronze():
    return(spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/civilwar_raw_data/civilwarorg_battles.csv"))

@dp.table(
    name="commanders_bronze",
    comment="Create materialized view from commander dataset"
)
@dp.expect("valid_belligerent", "belligerent in ('US', 'Confederate')")
def commanders_bronze():
    return(spark.read.format("csv").option("header", "true").load('/Volumes/workspace/default/civilwar_raw_data/civilwarorg_commanders.csv'))

#Explicitly define schema
forces_schema = StructType([
    StructField("battle_id", StringType(), True),
    StructField("belligerent", StringType(), True),
    StructField("strength", IntegerType(), True),
    StructField("casualties", IntegerType(), True),
    StructField("killed", IntegerType(), True),
    StructField("wounded", IntegerType(), True),
    StructField("killed_wounded", IntegerType(), True),
    StructField("missing_captured", IntegerType(), True)
])

@dp.table(
    name="forces_bronze",
    comment="Create materialized view from forces dataset",
    schema=forces_schema
)

def forces_bronze():
    return(spark.read.format("csv").option("header", "true").schema(forces_schema).load('/Volumes/workspace/default/civilwar_raw_data/civilwarorg_forces.csv'))
