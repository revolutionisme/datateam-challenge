package com.travelAudience.locator

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.travelAudience.locator.calculator.{BoundarySearch, HarvesineFormula}
import com.travelAudience.locator.config.JobConfig
import com.travelAudience.locator.model.{Coordinate, UserAirport, UserGeoLocation}
import org.scalatest.FunSuite

class NearestAirportFinderTest extends FunSuite with DatasetSuiteBase{

  test("FindNearestAirportToUser") {
    import spark.implicits._
    val config = new JobConfig(spark)
    val distance = new HarvesineFormula
    val airports = config.getAirportEvents
    val users = Seq(
      UserGeoLocation("Airplus International GmbH", Coordinate(50.0453634,8.6838193)),
      UserGeoLocation("Qimia GmbH", Coordinate(50.9416672,6.9343189)),
      UserGeoLocation("India Gate, Delhi", Coordinate(28.6075957,77.2164514))
    ).toDS

    val userWithNearestAirport = BoundarySearch.getUserWithNearestAirport(config, users, airports, distance, spark)

    val expectedAirportForUsers = Seq(
      UserAirport("Airplus International GmbH","FRA"),
      UserAirport("Qimia GmbH","CGN"),
      UserAirport("India Gate, Delhi","DEL")
    ).toDS()

    assertDatasetEquals(expectedAirportForUsers,userWithNearestAirport)
  }
}
