from pyspark.sql.functions import *
from pyspark import pipelines as dp
from transformation_code.bronze_to_silver_transforms import clean_forces_data, clean_commanders_data

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
    
    union_forces=clean_forces_data(forces_bronze,"union")
    confederate_forces=clean_forces_data(forces_bronze,"confederate")

    #Combine Forces datasets
    cleaned_forces=confederate_forces.join(union_forces,on="battle_id",how="inner")

    ####Clean commanders Table#########
    #Get Union and Confederate commanders separately
    union_commanders=clean_commanders_data(commanders_bronze,"union")
    confederate_commanders=clean_commanders_data(commanders_bronze,"confederate")
    
    #Combine Commanders Tables
    cleaned_commanders=confederate_commanders.join(union_commanders,on="battle_id",how="inner")

    #Join battle table to cleaned commander and forces tables
    return(
        battles_bronze.join(cleaned_forces, on='battle_id', how='inner').join(cleaned_commanders, on='battle_id', how='inner')
        )