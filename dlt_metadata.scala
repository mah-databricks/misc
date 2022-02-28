// Databricks notebook source
// DBTITLE 1,Preliminaries
import scala.util.matching.Regex

import org.apache.spark.sql.{Column, DataFrame, functions => F}

dbutils.widgets.text("dir_base", "/pipelines/", "Enter a Base Directory.")
dbutils.widgets.text("search", "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "Enter search regex.")
dbutils.widgets.text("dir_suff", "/system/events", "Enter suffix Directory.")

// COMMAND ----------

// DBTITLE 1,Get Locations
val base = dbutils.widgets.get("dir_base")
val suffix = dbutils.widgets.get("dir_suff")
val regex = dbutils.widgets.get("search")

val files = dbutils.fs.ls(base)
val locations = files.map{file =>
  val name = (regex.r).findFirstIn(file.name)
  name
}.collect{
  case Some(name) => base + name + suffix
}
    
println(locations)

// COMMAND ----------

// DBTITLE 1,Merge Metadata to Delta
def read(a: String) = spark.read.format("delta").load(a)
def write(df: DataFrame, name: String) = df.write.format("delta").mode("overwrite").save("/pipelines/" + name)
val df = locations.map(read).reduce(_.union(_)).where("timestamp > current_date() - 30")

write(df, "/pipelines/metadata_all")

// COMMAND ----------

// DBTITLE 1,Expectations
val expectations = df.
  where(F.expr(""" details IS NOT NULL AND details LIKE '%"expectations":[%' """)).
  select($"timestamp", F.expr(""" from_json(details, schema_of_json('{"flow_progress":{"status":" ","metrics":{"num_output_rows":0},"data_quality":{"dropped_records":0,"expectations":[{"name":" ","dataset":" ","passed_records":0,"failed_records":0}]}}}')) AS details """)).
  select($"timestamp", F.expr(""" explode(details.flow_progress.data_quality.expectations) AS details """)).
  select($"timestamp", $"details.dataset", $"details.name", $"details.failed_records", $"details.passed_records")

write(expectations, "metadata_expectations")

display(expectations)

// COMMAND ----------

