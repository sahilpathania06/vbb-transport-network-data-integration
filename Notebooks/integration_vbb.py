# Databricks notebook source
# MAGIC %md
# MAGIC # Integrating the Data and Performing Joins

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#Here the Data is ingested again so that it can support the integration in next steps

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
# MAGIC ##Performing inner Joins to get the common data

# COMMAND ----------

agency_routes_df = agency_df.join(
    routes_df,
    "agency_id",
    "inner"
)

agency_routes_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Checking if Join happens correctly

# COMMAND ----------

print("Total rows :- ", agency_routes_df.count()),
agency_routes_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining agency, routes and trips files

# COMMAND ----------

agency_routes_trips_df = agency_routes_df.join(
    trips_df,
    "route_id",
    "inner"
)

agency_routes_trips_df.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Counting total rows in join of agency, route and trips file

# COMMAND ----------

agency_routes_trips_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Performing Join between Agency, Routes, Trips and Stop_Times

# COMMAND ----------

agency_routes_trips_stop_times_df = agency_routes_trips_df.join(
    stop_times_df,
    "trip_id",
    "inner"
)

print("Total rows present :- ", agency_routes_trips_stop_times_df.count())
agency_routes_trips_stop_times_df.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Performing the Final Join between all the files (Agency, Routes, Trips, Stops and stop_times)

# COMMAND ----------

#Performing Final Join

final_df = agency_routes_trips_stop_times_df.join(
    stops_df,
    "stop_id",
    "inner"
)

print("Total rows present :- ", final_df.count())
final_df.show(5,truncate=False)
#