package com.linovi.spark.ex1

/**
  * Created by falga on 11/1/2016.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

object WordCount {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "WordCount", System.getenv("SPARK_HOME"))
    val rawWordCount = sc.textFile("wordcount/words.txt")
    val wordsRDD = rawWordCount.flatMap(line => line.split(" "))
    val groupedWordsRDD = wordsRDD.countByValue()
    groupedWordsRDD.foreach(println)
    sc.stop()
  }

}
