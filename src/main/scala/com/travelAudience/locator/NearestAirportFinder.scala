package com.travelAudience.locator

import org.apache.spark.sql.SparkSession

/**
  * Author: biplo on 08-05-2018.
  */
object NearestAirportFinder {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Travek Audience Nearest Airport Locator")
      .master("local[*]")
      .getOrCreate()


  }

}
