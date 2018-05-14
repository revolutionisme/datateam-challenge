package com.travelAudience.locator.config

import com.travelAudience.locator.model.{AirportGeoLocation, Coordinate, UserAirport, UserGeoLocation}
import com.travelAudience.locator.util.Constants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class JobConfig(sparkSession: SparkSession, fileNameOption: Option[String] = None) extends Serializable {

  import sparkSession.implicits._

  //Loads application.conf by default
  val config: Config = fileNameOption.fold(
    ifEmpty = ConfigFactory.load())(
    fileName => ConfigFactory.load(fileName))

  private def populateAirportArray(airportArray: Array[Array[List[AirportGeoLocation]]], airports: Array[AirportGeoLocation]): Array[Array[List[AirportGeoLocation]]] = {
    airports.foreach(airport => {
      val arrayLocation = getCorrectLocation(airport.coordinate)
      if(airportArray(arrayLocation._1)(arrayLocation._2) == null){
        airportArray(arrayLocation._1)(arrayLocation._2) = List(airport)
      } else {
        airportArray(arrayLocation._1)(arrayLocation._2) = airport :: airportArray(arrayLocation._1)(arrayLocation._2)
      }
    })
    airportArray
  }

  def getCorrectLocation(coordinate: Coordinate):(Int, Int) = {
    (coordinate.latitude.toInt + 90, coordinate.longitude.toInt + 180)
  }

  def getAirportEvents: Array[Array[List[AirportGeoLocation]]] = {
    val airports = sparkSession.read
      .option("header", "true")
      .csv(config.getString(Constants.AIRPORT_EVENTS))
      .map(row => {
        val iataCode = row.getAs[String]("iata_code")
        val coordinates = Coordinate(
          row.getAs[String]("latitude").toDouble,
          row.getAs[String]("longitude").toDouble
        )
        (iataCode, coordinates)
      }
      )
      .select('_1 as "iataCode", '_2 as "coordinate")
      .as[AirportGeoLocation]
      .collect()

    val airportArray = Array.ofDim[List[AirportGeoLocation]](181, 361)

    populateAirportArray(airportArray, airports)
  }

  def getUserEvents: Dataset[UserGeoLocation] = {
    sparkSession.read
      .option("header", "true")
      .csv(config.getString(Constants.USER_EVENTS))
      .map(row => {
        val iataCode = row.getAs[String]("uuid")
        val coordinates = Coordinate(
          row.getAs[String]("geoip_latitude").toDouble,
          row.getAs[String]("geoip_longitude").toDouble
        )
        (iataCode, coordinates)
      })
      .select('_1 as "uuid", '_2 as "coordinate")
      .as[UserGeoLocation]
  }

  def writeNearestAirport(outputData: Dataset[UserAirport]): Unit = {
    outputData
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(config.getString(Constants.NEAREST_AIRPORTS))
  }
}
