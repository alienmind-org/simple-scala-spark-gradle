package com.tutorial.spark

import org.apache.spark.sql.SparkSession

object SimpleSparkJob {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val spark = SparkSession.builder
                  .master("yarn")
                  .master("local[*]")
                  //.config("hadoop.security.authentication", "kerberos")
                  //.config("hadoop.rpc.protection", "privacy")
                  //.enableHiveSupport()
                  .appName("Simple Application")
                  .getOrCreate()
          
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}