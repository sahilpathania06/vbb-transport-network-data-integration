# Databricks notebook source
# MAGIC %md
# MAGIC #Finding Business insights through the files uploaded by apply spark sql querries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting the file to support other operations in this notebook

# COMMAND ----------

# creating base location for all the data and then combining files

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

agency_routes_df = agency_df.join(routes_df,"agency_id", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Which agencies has maximum routes according to the dataset?

# COMMAND ----------

from pyspark.sql.functions import count, desc

top_agencies_by_routes = agency_routes_df.groupBy("agency_name").agg(count("route_id").alias("Total_routes")
).orderBy(desc("Total_routes"))

top_agencies_by_routes.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Which routes have higher number of trips in VBB transport network?

# COMMAND ----------

agency_routes_trips_df = agency_routes_df.join(trips_df,"route_id", "inner")

from pyspark.sql.functions import count, desc

top_routes_by_trips = agency_routes_trips_df.groupBy("route_id","route_short_name"
).agg(count("trip_id").alias("Total_Trips")
).orderBy(desc("Total_Trips"))

top_routes_by_trips.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Which stops are mostly visited by the vehicles across whole VBB network?

# COMMAND ----------

from pyspark.sql.functions import desc, count

most_occupied_stops = stop_times_df.join(stops_df,"stop_id", "inner"
                                         ).groupBy("stop_id","stop_name"
                                        ).agg(count("*").alias("total_visits")
                                        ).orderBy(
                                            desc("total_visits")
                                        )
most_occupied_stops.show(15, truncate = False)                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Which sort of vehicles are most commen in the VBB Network?

# COMMAND ----------

routes_df.select("route_type"
).distinct().orderBy("route_type"
).show(50, False)

# COMMAND ----------

routes_df.createOrReplaceTempView("routes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC CASE
# MAGIC WHEN route_type = 3 THEN 'Tram'
# MAGIC WHEN route_type = 100 THEN 'Railway'
# MAGIC WHEN route_type = 106 THEN 'Regional Railway'
# MAGIC WHEN route_type = 109 THEN 'S-bahn'
# MAGIC WHEN route_type = 400 THEN 'Metro'
# MAGIC WHEN route_type = 700 THEN 'Bus'
# MAGIC WHEN route_type = 900 THEN 'Tram'
# MAGIC WHEN route_type = 1000 THEN 'Ferry'
# MAGIC ELSE 'Other'
# MAGIC END AS vehicle_type,
# MAGIC
# MAGIC COUNT(route_id) as Total_routes
# MAGIC
# MAGIC FROM routes
# MAGIC
# MAGIC GROUP BY
# MAGIC CASE
# MAGIC WHEN route_type = 3 THEN 'Tram'
# MAGIC WHEN route_type = 100 THEN 'Railway'
# MAGIC WHEN route_type = 106 THEN 'Regional Railway'
# MAGIC WHEN route_type = 109 THEN 'S-bahn'
# MAGIC WHEN route_type = 400 THEN 'Metro'
# MAGIC WHEN route_type = 700 THEN 'Bus'
# MAGIC WHEN route_type = 900 THEN 'Tram'
# MAGIC WHEN route_type = 1000 THEN 'Ferry'
# MAGIC ELSE 'Other'
# MAGIC END
# MAGIC
# MAGIC ORDER BY total_routes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Which stop has the most unique routes?

# COMMAND ----------

from pyspark.sql.functions import countDistinct, desc

transport_hub = stop_times_df.join(
    trips_df, "trip_id", "inner"
).join(
    stops_df, "stop_id", "inner"
).groupBy(
    "stop_id", "stop_name"
).agg(
    countDistinct("route_id").alias("unique_routes")
).orderBy(
    desc("unique_routes")
)

transport_hub.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Are all the routes mapped to valid agency?

# COMMAND ----------

valid_routes = routes_df.join(
    agency_df,
    "agency_id",
    "left_semi"
)

print("Valid Routes :- ", valid_routes.count())

print("Total Routes :- ", routes_df.count())