# Databricks notebook source
# MAGIC %md
# MAGIC # In this file we have evaluated if there are some duplications in the file that were imported or if there were any null values
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Loading the files again for reference of other operations

# COMMAND ----------

#I have loaded the files again for the reference of other operations like null values, duplications

base_location = "/Volumes/vbb_project/raw_data/vbb_data/"

def load_gtfs_file(file_name):
    return spark.read.csv(
        base_location + file_name,
        header = True,
        inferSchema = True
    )

agency_df = load_gtfs_file("agency.txt")
routes_df = load_gtfs_file("routes.txt")
trips_df = load_gtfs_file("trips.txt")
stops_df = load_gtfs_file("stops.txt")
stop_times_df = load_gtfs_file("stop_times.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Counting total rows in the files

# COMMAND ----------

print("Agency :- ", agency_df.count())
print("Routes :- ", routes_df.count())
print("Trips :- ", trips_df.count())
print("Stops :- ", stops_df.count())
print("Stop Times :- ", stop_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Counting total unique rows from all the files by removing the duplicates

# COMMAND ----------

print("Agency unique rows :- ",agency_df.dropDuplicates().count())
print("Routes unique rows :- ",routes_df.dropDuplicates().count())
print("Trips unique rows :- ",trips_df.dropDuplicates().count())
print("Stops unique rows :- ",stops_df.dropDuplicates().count())
print("Stop Times unique rows :- ",stop_times_df.dropDuplicates().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Checking the number of null values in the files

# COMMAND ----------

from pyspark.sql.functions import col, count, when

agency_df.select([
    count(when(col(c).isNull(),c)).alias(c)
    for c in agency_df.columns
]).show()

routes_df.select([
    count(when(col(c).isNull(),c)).alias(c)
    for c in routes_df.columns
]).show()

trips_df.select([
    count(when(col(c).isNull(),c)).alias(c)
    for c in trips_df.columns
]).show()

stops_df.select([
    count(when(col(c).isNull(),c)).alias(c)
    for c in stops_df.columns
]).show()

stop_times_df.select([
    count(when(col(c).isNull(),c)).alias(c)
    for c in stop_times_df.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking if Foreign key is true and integration between the files are actually working

# COMMAND ----------

## 5. Examining if Integration Checks within files are actually correct

missing_agency = routes_df.join(
    agency_df,
    "agency_id",
    "left_anti"
)

missing_routes = trips_df.join(
    routes_df,
    "route_id",
    "left_anti"
)

missing_trips = stop_times_df.join(
    trips_df,
    "trip_id",
    "left_anti"
)

missing_stops = stop_times_df.join(
    stops_df,
    "stop_id",
    "left_anti"
)

print("Total number of routes which have invalid agency_id :- ", missing_agency.count())
print("Total number of trips which have invalid route_id :- ", missing_routes.count())
print("Total number of stops_times which have invalid trip_id :- ", missing_trips.count())
print("Total number of stops_times which have invalid stop_id :- ", missing_stops.count())