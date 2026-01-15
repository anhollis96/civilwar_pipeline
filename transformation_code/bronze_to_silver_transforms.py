from pyspark.sql.functions import col, count, lit, when

def clean_forces_data(forces_bronze,belligerent="union"):
  #Filter forces data to include only battles with strength and casualties numbers
  forces_filtered = forces_bronze.filter((col("strength").isNotNull()) & (col("casualties").isNotNull() | (col("killed").isNotNull()) | (col("wounded").isNotNull()) | (col("killed_wounded").isNotNull()) | (col("missing_captured").isNotNull())))
  
  #Set null casualty values to zero
  forces_filtered=forces_filtered.fillna(0,subset=["killed","wounded","killed_wounded","missing_captured"])

  #Clean Casualties field
  forces_filtered=forces_filtered.withColumn("casualties",when(col("casualties").isNull(),col("killed")+col("wounded")+col("killed_wounded")+col("missing_captured")).otherwise(col("casualties"))).drop("killed","wounded","killed_wounded","missing_captured")

  if belligerent=="union":
    #Get Union Specific data
    union_forces=forces_filtered.filter(col("belligerent")=="US")
    union_forces=union_forces.withColumnRenamed("strength","union_strength").withColumnRenamed("casualties","union_casualties").drop("belligerent")
    return(union_forces)
  else:
    #Get Confederate Specific data
    confederate_forces=forces_filtered.filter(col("belligerent")=="Confederate")
    confederate_forces=confederate_forces.withColumnRenamed("strength","confederate_strength").withColumnRenamed("casualties","confederate_casualties").drop("belligerent")
    return(confederate_forces)

def clean_commanders_data(commanders_bronze,belligerent="union"):
  if belligerent=="union":
    union_commanders=commanders_bronze.filter(col("belligerent")=="US")
    union_commanders=union_commanders.withColumnRenamed("name","union_commander").drop("belligerent").drop("url")
    return(union_commanders)
  else:
    confederate_commanders=commanders_bronze.filter(col("belligerent")=="Confederate")
    confederate_commanders=confederate_commanders.withColumnRenamed("name","confederate_commander").drop("belligerent").drop("url")
    return(confederate_commanders)









