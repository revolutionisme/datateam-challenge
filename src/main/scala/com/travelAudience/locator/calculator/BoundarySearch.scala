package com.travelAudience.locator.calculator

import com.travelAudience.locator.config.JobConfig
import com.travelAudience.locator.model.{AirportGeoLocation, UserAirport, UserGeoLocation}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object BoundarySearch {

  def calculateNearestAirport(sparkSession: SparkSession, config: JobConfig): Unit = {

    val airportsArray = config.getAirportEvents
    val users = config.getUserEvents
    val broadcastedAirportsArray = sparkSession.sparkContext.broadcast(airportsArray).value
    val distance = new HarvesineFormula

    val usersWithNearestAirports = getUserWithNearestAirport(config, users, broadcastedAirportsArray, distance, sparkSession)

    config.writeNearestAirport(usersWithNearestAirports)
  }

  def getUserWithNearestAirport(config: JobConfig,
                                users:Dataset[UserGeoLocation],
                                broadcastedAirportsArray: Array[Array[List[AirportGeoLocation]]],
                                distance: HarvesineFormula,
                                sparkSession: SparkSession
                               ):Dataset[UserAirport] = {

    import sparkSession.implicits._

    users.mapPartitions(userEvents => {
      userEvents.map(userEvent => {
        val userArrayCoordinate = config.getCorrectLocation(userEvent.coordinate)
        var allnearbyAirports = ListBuffer[AirportGeoLocation]()
        val boundary = ListBuffer[Int](0, +1, -1)
        var positiveExtent = +1
        var negativeExtent = -1
        while (allnearbyAirports.isEmpty) {
          //return coordinates for checked locations, and send over those locations
          allnearbyAirports = getOnlyAllNearbyAirports(userArrayCoordinate, broadcastedAirportsArray, boundary)
          positiveExtent = positiveExtent + 1
          negativeExtent = negativeExtent - 1
          // Increase boundary to check after each iteration which returns empty airport list
          boundary.append(positiveExtent, negativeExtent)
        }
        val nearestAirportToUser = allnearbyAirports.minBy(airport => distance.calculateDistanceFor2Coordinates(airport.coordinate, userEvent.coordinate))
        (userEvent.uuid, nearestAirportToUser.iataCode)
      })
    })
      .select('_1 as "uuid", '_2 as "iataCode")
      .as[UserAirport]
  }

  /**
    * Checks within the boundary for existing airports closest to the user
    *
    * @param userArrayCoordinate coordinates according to the array for the user
    * @param airportsArray the array where each entity holds a list of airports
    * @param boundary the boundary to check in
    * @return list of nearby airport to the given userArrayCoordinate
    */
  private def getOnlyAllNearbyAirports(userArrayCoordinate: (Int, Int), airportsArray: Array[Array[List[AirportGeoLocation]]], boundary: ListBuffer[Int]): ListBuffer[AirportGeoLocation] = {
    val nearbyAirports = ListBuffer[AirportGeoLocation]()

    for (latB <- boundary) {
      for (longB <- boundary) {
        if (airportsArray(userArrayCoordinate._1 + latB)(userArrayCoordinate._2 + longB) != null) {
          nearbyAirports.appendAll(airportsArray(userArrayCoordinate._1 + latB)(userArrayCoordinate._2 + longB).to[ListBuffer])
        }
      }
    }
    nearbyAirports
  }

}
