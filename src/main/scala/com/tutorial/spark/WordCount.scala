package com.tutorial.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    // adapted from wiki: https://en.wikipedia.org/wiki/Apache_Spark
    val conf = new SparkConf().setAppName("wiki_test") // create a spark config object
    val sc = new SparkContext(conf) // Create a spark context
    val data = sc.textFile("/Users/yanggao/tools/spark-3.0.0-bin-hadoop2.7/README.md") // Read files from "somedir" into an RDD of (filename, content) pairs.
    val tokens = data.flatMap(_.split(" ")) // Split each file into a list of tokens (words).
    val wordFreq = tokens.map((_, 1)).reduceByKey(_ + _) // Add a count of one to each token, then sum the counts per word type.
    wordFreq.sortBy(s => -s._2).map(x => (x._2, x._1)).top(10) // Get the top 10 words. Swap word and count to sort by count.
    println(s"================\nword frequency as map: ${wordFreq.collectAsMap().toString()}")
  }
}
