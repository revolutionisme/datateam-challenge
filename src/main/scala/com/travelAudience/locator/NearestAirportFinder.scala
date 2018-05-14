package com.travelAudience.locator

import com.travelAudience.locator.calculator.{BoundarySearch, HarvesineFormula}
import com.travelAudience.locator.config.JobConfig
import com.travelAudience.locator.model.{AirportGeoLocation, UserAirport}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Author: biplo on 08-05-2018.
  */
object NearestAirportFinder {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Travel Audience Nearest Airport Locator")
      .master("local[4]")
      .getOrCreate()

    val config = new JobConfig(sparkSession)
    BoundarySearch.calculateNearestAirport(sparkSession, config)
  }

}
