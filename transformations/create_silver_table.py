from pyspark.sql.functions import *
from pyspark import pipelines as dp

#Create Final silver battles table

@dp.table(
    name="battles_silver",
    comment="Create materialized view from bronze datasets"
)

def battles_silver():
    #Read in bronze tables
    battles_bronze=spark.read.table("workspace.civilwar2.battles_bronze")
    forces_bronze=spark.read.table("workspace.civilwar2.forces_bronze")
    commanders_bronze=spark.read.table("workspace.civilwar2.commanders_bronze")
    
    ####Clean Forces Table###
    #Filter forces data to include only battles with strength and casualties numbers
    forces_filtered = forces_bronze.filter((col("strength").isNotNull()) & (col("casualties").isNotNull() | (col("killed").isNotNull()) | (col("wounded").isNotNull()) | (col("killed_wounded").isNotNull()) | (col("missing_captured").isNotNull())))

    #Set null casualty values to zero
    forces_filtered=forces_filtered.fillna(0,subset=["killed","wounded","killed_wounded","missing_captured"])

    #Clean Casualties field
    forces_filtered=forces_filtered.withColumn("casualties",when(col("casualties").isNull(),col("killed")+col("wounded")+col("killed_wounded")+col("missing_captured")).otherwise(col("casualties"))).drop("killed","wounded","killed_wounded","missing_captured")

    #Get Union Specific data
    union_forces=forces_filtered.filter(col("belligerent")=="US")
    union_forces=union_forces.withColumnRenamed("strength","union_strength").withColumnRenamed("casualties","union_casualties").withColumnRenamed("killed","union_killed").withColumnRenamed("wounded","union_wounded").withColumnRenamed("killed_wounded","union_killed_wounded").withColumnRenamed("missing_captured","union_missing_captured").drop("belligerent")

    #Get Confederate Specific data
    confederate_forces=forces_filtered.filter(col("belligerent")=="Confederate")
    confederate_forces=confederate_forces.withColumnRenamed("strength","confederate_strength").withColumnRenamed("casualties","confederate_casualties").withColumnRenamed("killed","confederate_killed").withColumnRenamed("wounded","confederate_wounded").withColumnRenamed("killed_wounded","confederate_killed_wounded").withColumnRenamed("missing_captured","confederate_missing_captured").drop("belligerent")

    #Combine Forces datasets
    cleaned_forces=confederate_forces.join(union_forces,on="battle_id",how="inner")

    ####Clean commanders Table#########
    #Get Union and Confederate commanders separately
    confederate_commanders=commanders_bronze.filter(col("belligerent")=="Confederate")
    confederate_commanders=confederate_commanders.withColumnRenamed("name","confederate_commander").drop("belligerent").drop("url")
    union_commanders=commanders_bronze.filter(col("belligerent")=="US")
    union_commanders=union_commanders.withColumnRenamed("name","union_commander").drop("belligerent").drop("url")

    #Combine Commanders Tables
    cleaned_commanders=confederate_commanders.join(union_commanders,on="battle_id",how="inner")

    #Join battle table to cleaned commander and forces tables
    return(
        battles_bronze.join(cleaned_forces, on='battle_id', how='inner').join(cleaned_commanders, on='battle_id', how='inner')
        )