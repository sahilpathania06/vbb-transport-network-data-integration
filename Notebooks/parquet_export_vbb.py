# Databricks notebook source
# MAGIC %md
# MAGIC #Recreating final integrated dataframe

# COMMAND ----------

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

agency_routes_df = agency_df.join(
    routes_df,
    "agency_id",
    "inner"
)

agency_routes_trips_df = agency_routes_df.join(
    trips_df,
    "route_id",
    "inner"
)

agency_routes_trips_stop_times_df = agency_routes_trips_df.join(
    stop_times_df,
    "trip_id",
    "inner"
)

final_integrated_df = agency_routes_trips_stop_times_df.join(
    stops_df,
    "stop_id",
    "inner"
)


# COMMAND ----------

final_integrated_df.write.mode("overwrite").parquet(
    "/Volumes/vbb_project/raw_data/vbb_data/processed/final_integrated_parquet"
)

# COMMAND ----------

parquet_df = spark.read.parquet(
    "/Volumes/vbb_project/raw_data/vbb_data/processed/final_integrated_parquet"
)

print("Total rows :- ",parquet_df.count())
parquet_df.show(5, truncate = False)