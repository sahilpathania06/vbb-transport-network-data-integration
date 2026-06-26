# Databricks notebook source
# MAGIC %md
# MAGIC #Defining the function for ingestion

# COMMAND ----------

#Defining the function for ingesti

base_location = "/Volumes/vbb_project/raw_data/vbb_data/"

def load_gtfs_file(file_name):
    return spark.read.csv(
        base_location + file_name,
        header = True,
        inferSchema = True
    )

#loading the files as db frames

agency_df = load_gtfs_file("agency.txt")
routes_df = load_gtfs_file("routes.txt")
trips_df = load_gtfs_file("trips.txt")
stops_df = load_gtfs_file("stops.txt")
stop_times_df = load_gtfs_file("stop_times.txt")

# COMMAND ----------

agency_df.show()
routes_df.show()
trips_df.show()
stops_df.show()
stop_times_df.show()

# COMMAND ----------

agency_df.printSchema()
routes_df.printSchema()
trips_df.printSchema()
stops_df.printSchema()
stop_times_df.printSchema()