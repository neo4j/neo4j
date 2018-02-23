/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.values.storable.{CoordinateReferenceSystem, Values}

class SpatialFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  val pointConfig = Configs.Interpreted - Configs.Version2_3
  val equalityConfig = Configs.Interpreted - Configs.OldAndRule

  test("point function should work with literal map") {
    val result = executeWith(pointConfig, "RETURN point({latitude: 12.78, longitude: 56.7}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with literal map and cartesian coordinates") {
    val result = executeWith(pointConfig, "RETURN point({x: 2.3, y: 4.5, crs: 'cartesian'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5))))
  }

  test("point function should work with literal map and geographic coordinates") {
    val result = executeWith(pointConfig, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
      expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("point function should not work with literal map and incorrect cartesian CRS") {
    failWithError(pointConfig, "RETURN point({x: 2.3, y: 4.5, crs: 'cart'}) as point", List("'cart' is not a supported coordinate reference system for points",
      "Unknown coordinate reference system: cart"))
  }

  test("point function should not work with literal map and incorrect geographic CRS") {
    failWithError(pointConfig, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS84'}) as point", List("'WGS84' is not a supported coordinate reference system for points",
      "Unknown coordinate reference system: WGS84"))
  }

  test("point function should work with integer arguments") {
    val result = executeWith(pointConfig, "RETURN point({x: 2, y: 4}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2, 4))))
  }

  test("should fail properly if missing cartesian coordinates") {
    failWithError(pointConfig, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("y" -> 1.0, "crs" -> "cartesian"))
  }

  test("should fail properly if missing geographic longitude") {
    failWithError(pointConfig, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("latitude" -> 1.0, "crs" -> "WGS-84"))
  }

  test("should fail properly if missing geographic latitude") {
    failWithError(pointConfig, "RETURN point({params}) as point", List("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'"),
      params = "params" -> Map("longitude" -> 1.0, "crs" -> "WGS-84"))
  }

  test("should fail properly if unknown coordinate system") {
    failWithError(pointConfig, "RETURN point({params}) as point", List("'WGS-1337' is not a supported coordinate reference system for points",
      "Unknown coordinate reference system: WGS-1337"),
      params = "params" -> Map("x" -> 1, "y" -> 2, "crs" -> "WGS-1337"))
  }

  test("should default to Cartesian if missing cartesian CRS") {
    val result = executeWith(pointConfig, "RETURN point({x: 2.3, y: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5))))
  }

  test("should default to WGS84 if missing geographic CRS") {
    val result = executeWith(pointConfig, "RETURN point({longitude: 2.3, latitude: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("should allow Geographic CRS with x/y coordinates") {
    val result = executeWith(pointConfig, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("should not allow Cartesian CRS with latitude/longitude coordinates") {
    failWithError(pointConfig, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'cartesian'}) as point",
      List("'cartesian' is not a supported coordinate reference system for geographic points",
        "Geographic points does not support coordinate reference system: cartesian"))
  }

  test("point function should work with previous map") {
    val result = executeWith(pointConfig, "WITH {latitude: 12.78, longitude: 56.7} as data RETURN point(data) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node properties") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(pointConfig - Configs.Morsel, "MATCH (p:Place) RETURN point({latitude: p.latitude, longitude: p.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with relationship properties") {
    // Given
    val r = relate(createNode(), createNode(), "PASS_THROUGH", Map("latitude" -> 12.78, "longitude" -> 56.7))

    // When
    val result = executeWith(pointConfig, "MATCH ()-[r:PASS_THROUGH]->() RETURN point({latitude: r.latitude, longitude: r.longitude}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node as map") {
    // Given
    createLabeledNode(Map("latitude" -> 12.78, "longitude" -> 56.7), "Place")

    // When
    val result = executeWith(pointConfig - Configs.Morsel, "MATCH (p:Place) RETURN point(p) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with null input") {
    val result = executeWith(pointConfig, "RETURN point(null) as p")
    result.toList should equal(List(Map("p" -> null)))
  }

  test("point function should return null if the map that backs it up contains a null") {
    var result = executeWith(pointConfig, "RETURN point({latitude:null, longitude:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(pointConfig, "RETURN point({latitude:3, longitude:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(pointConfig, "RETURN point({x:null, y:3}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))

    result = executeWith(pointConfig, "RETURN point({x:3, y:null}) as pt;")
    result.toList should equal(List(Map("pt" -> null)))
  }

  test("point function should fail on wrong type") {
    val config = Configs.AbsolutelyAll + TestConfiguration(Versions.Default, Planners.Default, Runtimes.Default) - Configs.Version2_3
    failWithError(config, "RETURN point(1) as dist", List("Type mismatch: expected Map, Node or Relationship but was Integer"))
  }

  test("point should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val result = executeWith(equalityConfig, "MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("point should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("array of points should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val query =
      """
        |UNWIND [1,2,3] as num
        |WITH point({x: num, y: num}) as p
        |WITH collect(p) as points
        |MATCH (place:Place) SET place.location = points
        |RETURN points
      """.stripMargin
    val result = executeWith(equalityConfig, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("points" -> List(
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 3.0, 3.0)
    ))))
  }

  test("array of cartesian points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute(
      """
        |UNWIND [1,2,3] as num
        |WITH point({x: num, y: num}) as p
        |WITH collect(p) as points
        |MATCH (place:Place) SET place.location = points
        |RETURN place.location as points
      """.stripMargin)

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 3.0, 3.0)
    ))
  }

  test("array of wgs84 points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute(
      """
        |UNWIND [1,2,3] as num
        |WITH point({latitude: num, longitude: num}) as p
        |WITH collect(p) as points
        |MATCH (place:Place) SET place.location = points
        |RETURN place.location as points
      """.stripMargin)

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as points",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    val points = result.columnAs("points").toList.head.asInstanceOf[Array[_]]
    points should equal(Array(
      Values.pointValue(CoordinateReferenceSystem.WGS84, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 3.0, 3.0)
    ))
  }

  test("array of mixed points should not be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val query =
      """
        |WITH [point({x: 1, y: 2}), point({latitude: 1, longitude: 2})] as points
        |MATCH (place:Place) SET place.location = points
        |RETURN points
      """.stripMargin

    // Then
    failWithError(equalityConfig + Configs.Procs, query, Seq("Collections containing point values with different CRS can not be stored in properties."))
  }

}
