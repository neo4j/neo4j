/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CRS, CartesianPoint, GeographicPoint}
import org.neo4j.cypher.{ExecutionEngineFunSuite}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Versions.{V3_1, V3_2}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class SpatialFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  val expectedToSucceed = Configs.Interpreted - Configs.Version2_3

  val expectedToFail = TestConfiguration(Versions(Versions.Default, V3_1, V3_2), Planners.all, Runtimes.Default)

  test("point function should work with literal map") {
    val result = executeWith(expectedToSucceed, "RETURN point({latitude: 12.78, longitude: 56.7}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with literal map and cartesian coordinates") {
    val result = executeWith(expectedToSucceed, "RETURN point({x: 2.3, y: 4.5, crs: 'cartesian'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> CartesianPoint(2.3, 4.5, CRS.Cartesian))))
  }

  test("point function should work with literal map and geographic coordinates") {
    val result = executeWith(expectedToSucceed, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
      expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("point function should not work with literal map and incorrect cartesian CRS") {
    failWithError(expectedToFail, "RETURN point({x: 2.3, y: 4.5, crs: 'cart'}) as point", List("'cart' is not a supported coordinate reference system for points"))
  }

  test("point function should not work with literal map and incorrect geographic CRS") {
    failWithError(expectedToFail, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS84'}) as point", List("'WGS84' is not a supported coordinate reference system for points"))
  }

  test("point function should work with integer arguments") {
    val result = executeWith(expectedToSucceed, "RETURN point({x: 2, y: 4}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> CartesianPoint(2, 4, CRS.Cartesian))))
  }

  test("should fail properly if missing cartesian coordinates") {
    failWithError(expectedToFail, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("y" -> 1.0, "crs" -> "cartesian"))
  }

  test("should fail properly if missing geographic longitude") {
    failWithError(expectedToFail, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("latitude" -> 1.0, "crs" -> "WGS-84"))
  }

  test("should fail properly if missing geographic latitude") {
    failWithError(expectedToFail, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("longitude" -> 1.0, "crs" -> "WGS-84"))
  }

  test("should fail properly if unknown coordinate system") {
    failWithError(expectedToFail, "RETURN point({params}) as point", List("'WGS-1337' is not a supported coordinate reference system for points"),
      params = "params" -> Map("x" -> 1, "y" -> 2, "crs" -> "WGS-1337"))
  }

  test("should default to Cartesian if missing cartesian CRS") {
    val result = executeWith(expectedToSucceed, "RETURN point({x: 2.3, y: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> CartesianPoint(2.3, 4.5, CRS.Cartesian))))
  }

  test("should default to WGS84 if missing geographic CRS") {
    val result = executeWith(expectedToSucceed, "RETURN point({longitude: 2.3, latitude: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("should allow Geographic CRS with x/y coordinates") {
    val result = executeWith(expectedToSucceed, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> GeographicPoint(2.3, 4.5, CRS.WGS84))))
  }

  test("should not allow Cartesian CRS with latitude/longitude coordinates") {
    failWithError(expectedToFail, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'cartesian'}) as point",
      List("'cartesian' is not a supported coordinate reference system for geographic points"))
  }

  test("point function should work with previous map") {
    val result = executeWith(expectedToSucceed, "WITH {latitude: 12.78, longitude: 56.7} as data RETURN point(data) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("distance function should work on co-located points") {
    val result = executeWith(expectedToSucceed, "WITH point({latitude: 12.78, longitude: 56.7}) as point RETURN distance(point,point) as dist",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point", "dist"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("dist" -> 0.0)))
  }

  test("distance function should work on nearby cartesian points") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({x: 2.3, y: 4.5, crs: 'cartesian'}) as p1, point({x: 1.1, y: 5.4, crs: 'cartesian'}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "p1", "p2", "dist"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.columnAs("dist").next().asInstanceOf[Double] should equal(1.5)
  }

  test("distance function should work on nearby points") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({longitude: 12.78, latitude: 56.7}) as p1, point({latitude: 56.71, longitude: 12.79}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "p1", "p2", "dist"),
        expectPlansToFail = Configs.AllRulePlanners))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(1270)
  }

  test("distance function should work on distant points") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({latitude: 56.7, longitude: 12.78}) as p1, point({longitude: -51.9, latitude: -16.7}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
    planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "p1", "p2", "dist"),
      expectPlansToFail = Configs.AllRulePlanners))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(10116214)
  }

  // Fails with TestFailException or java.lang.IllegalArgumentException depending on TestConfiguration
  test("distance function should fail if provided with points from different CRS") {
    try {
      failWithError(expectedToFail,
        """WITH point({x: 2.3, y: 4.5, crs: 'cartesian'}) as p1, point({longitude: 1.1, latitude: 5.4, crs: 'WGS-84'}) as p2
        |RETURN distance(p1,p2) as dist""".stripMargin, List("Invalid points passed to distance(p1, p2)"))
    } catch {
      case e: Throwable => assert(e.getMessage.contains("Invalid points passed to distance(p1, p2)"))
    }
  }

  test("distance function should measure distance from Copenhagen train station to Neo4j in Malmö") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1, point({latitude: 55.611784, longitude: 12.994341}) as p2
        |RETURN distance(p1,p2) as dist
      """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "p1", "p2","dist"),
        expectPlansToFail = Configs.AllRulePlanners))

    Math.round(result.columnAs("dist").next().asInstanceOf[Double]) should equal(27842)
  }

  test("distance function should work with two null inputs") {
    val result = executeWith(expectedToSucceed, "RETURN distance(null, null) as dist")
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with lhs null input") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
        |RETURN distance(null, p1) as dist
      """.stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null with rhs null input") {
    val result = executeWith(expectedToSucceed,
      """
        |WITH point({latitude: 55.672874, longitude: 12.564590}) as p1
        |RETURN distance(p1, null) as dist
      """.stripMargin)
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should return null if a point is null") {
    var result = executeWith(expectedToSucceed,
      "RETURN distance(point({latitude:3,longitude:7}),point({latitude:null, longitude:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(expectedToSucceed,
      "RETURN distance(point({latitude:3,longitude:null}),point({latitude:7, longitude:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(expectedToSucceed,
      "RETURN distance(point({x:3,y:7}),point({x:null, y:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))

    result = executeWith(expectedToSucceed,
      "RETURN distance(point({x:3,y:null}),point({x:7, y:3})) as dist;")
    result.toList should equal(List(Map("dist" -> null)))
  }

  test("distance function should fail on wrong type") {
    val config = Configs.AbsolutelyAll + TestConfiguration(Versions.Default, Planners.Default, Runtimes.Default) - Configs.Version2_3
    failWithError(config, "RETURN distance(1, 2) as dist", List("Type mismatch: expected Point or Geometry but was Integer"))
  }

  test("point function should work with node properties") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(expectedToSucceed, "MATCH (p:Place) RETURN point({latitude: p.latitude, longitude: p.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with relationship properties") {
    // Given
    val r = relate(createNode(), createNode(), "PASS_THROUGH", Map("latitude" -> 12.78, "longitude" -> 56.7))

    // When
    val result = executeWith(expectedToSucceed, "MATCH ()-[r:PASS_THROUGH]->() RETURN point({latitude: r.latitude, longitude: r.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with node as map") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(expectedToSucceed, "MATCH (p:Place) RETURN point(p) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }

  test("point function should work with null input") {
    val result = executeWith(expectedToSucceed, "RETURN point(null) as p")
    result.toList should equal(List(Map("p" -> null)))
  }

  test("point function should return null if the map that backs it up contains a null") {
    var result = executeWith(expectedToSucceed, "RETURN point({latitude:null, longitude:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(expectedToSucceed, "RETURN point({latitude:3, longitude:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(expectedToSucceed, "RETURN point({x:null, y:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(expectedToSucceed, "RETURN point({x:3, y:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))
  }

  test("point function should fail on wrong type") {
    val config = Configs.AbsolutelyAll + TestConfiguration(Versions.Default, Planners.Default, Runtimes.Default) - Configs.Version2_3
    failWithError(config, "RETURN point(1) as dist", List("Type mismatch: expected Map, Node or Relationship but was Integer"))
  }

  ignore("point function should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val result = executeWith(expectedToSucceed, "MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> GeographicPoint(56.7, 12.78, CRS.WGS84))))
  }
}
