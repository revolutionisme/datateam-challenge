package com.travelAudience.locator.calculator

import com.travelAudience.locator.model.Coordinate

/**
  * Author: biplo on 13-05-2018.
  */
abstract class Distance extends Serializable {

  def calculateDistanceFor2Coordinates(coordinate1: Coordinate, coordinate2: Coordinate): Double
}
