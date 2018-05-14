package com.travelAudience.locator

import com.travelAudience.locator.calculator.HarvesineDistance
import com.travelAudience.locator.config.JobConfig
import com.travelAudience.locator.model.{AirportGeoLocation, Coordinate, UserAirport, UserGeoLocation}
import com.typesafe.config.ConfigFactory

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark._

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
    calculateNearestAirport(sparkSession,config)
  }

  def calculateNearestAirport(sparkSession: SparkSession, config: JobConfig):Unit = {

    import sparkSession.implicits._
    val airportsArray = config.getAirportEvents
    val users = config.getUserEvents

    val broadcastedAirportsArray = sparkSession.sparkContext.broadcast(airportsArray).value
    val distance = new HarvesineDistance

    // This logic can be improved upon by using something like a quad tree
    // A crude
    val usersWithNearestAirports = users.mapPartitions(userEvents =>{
      userEvents.map(userEvent => {
        val userArrayCoordinate = config.getCorrectLocation(userEvent.coordinate)
        var allnearbyAirports = List[AirportGeoLocation]()
        var i = 1
        while (allnearbyAirports.isEmpty){
          //return coordinates for checked locations, and send over those locations
          allnearbyAirports = getOnlyAllNearbyAirports(userArrayCoordinate,broadcastedAirportsArray, i)
          i = i + 1
        }
        val nearestAirportToUser = allnearbyAirports.minBy(airport => distance.calculateDistanceFor2Coordinates(airport.coordinate,userEvent.coordinate))
        (userEvent.uuid, nearestAirportToUser.iataCode)
      })
    })
      .select('_1 as "uuid", '_2 as "iataCode")
      .as[UserAirport]

    config.writeNearestAirport(usersWithNearestAirports)
  }

  def getOnlyAllNearbyAirports(userCoordinate: (Int, Int), airportsArray:Array[Array[List[AirportGeoLocation]]], depth: Int):List[AirportGeoLocation] = {
    var nearbyAirports = airportsArray(userCoordinate._1)(userCoordinate._2)
    if(nearbyAirports == null) {
      nearbyAirports = List[AirportGeoLocation]()
    }

    //Check for  edge cases and correct logic for going into depth
    if(airportsArray(userCoordinate._1 - depth)(userCoordinate._2 - depth) != null) {
      nearbyAirports = nearbyAirports ::: airportsArray(userCoordinate._1 - depth)(userCoordinate._2 - depth)
    } else if(airportsArray(userCoordinate._1 - depth)(userCoordinate._2) != null){
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1 - depth)(userCoordinate._2)
    } else if(airportsArray(userCoordinate._1)(userCoordinate._2 - depth) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1)(userCoordinate._2 - depth)
    } else if(airportsArray(userCoordinate._1 + depth)(userCoordinate._2 + depth) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1 + depth)(userCoordinate._2 + depth)
    } else if(airportsArray(userCoordinate._1 + depth)(userCoordinate._2) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1 + depth)(userCoordinate._2)
    } else if(airportsArray(userCoordinate._1)(userCoordinate._2 + depth) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1)(userCoordinate._2 + depth)
    } else if(airportsArray(userCoordinate._1 - depth)(userCoordinate._2 + depth) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1 - depth)(userCoordinate._2 + depth)
    } else if(airportsArray(userCoordinate._1 + depth)(userCoordinate._2 - depth) != null) {
      nearbyAirports = nearbyAirports :::  airportsArray(userCoordinate._1 + depth)(userCoordinate._2 - depth)
    }

    nearbyAirports
  }

}
