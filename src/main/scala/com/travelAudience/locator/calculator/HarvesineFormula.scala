package com.travelAudience.locator.calculator
import com.travelAudience.locator.model.Coordinate

/**
  * Harvesine Distance Formulation obtained from
  * https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula
  * Originally in Javascript, Converted to Scala
  */
class HarvesineFormula extends Distance {

  override def calculateDistanceFor2Coordinates(coordinate1: Coordinate, coordinate2: Coordinate): Double = {
      val R = 6371; // Radius of the earth in km
      val degreeDistance = subtractCoordinates(coordinate2, coordinate1)
      val dLat = deg2rad(degreeDistance.latitude)  // deg2rad below
      val dLon = deg2rad(degreeDistance.longitude)
      val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
          Math.cos(deg2rad(coordinate1.longitude)) * Math.cos(deg2rad(coordinate2.latitude))*
          Math.sin(dLon/2) * Math.sin(dLon/2)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
      R * c; // Distance in km
    }

  def deg2rad(deg:Double):Double = {
    deg * (Math.PI/180)
  }

  def subtractCoordinates(coordinate2: Coordinate, coordinate1: Coordinate):Coordinate={
    val latitude = coordinate2.latitude - coordinate1.latitude
    val longitude = coordinate2.longitude - coordinate1.longitude
    Coordinate(latitude, longitude)
  }
}
