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
  val latestPointConfig = Configs.Interpreted - Configs.BackwardsCompatibility - Configs.AllRulePlanners

  test("toString on points") {
    executeWith(latestPointConfig, "RETURN toString(point({x:1, y:2})) AS s").toList should equal(List(Map("s" -> "point({x: 1.0, y: 2.0, crs: 'cartesian'})")))
    executeWith(latestPointConfig, "RETURN toString(point({longitude:1, latitude:2, height:3})) AS s").toList should equal(List(Map("s" -> "point({x: 1.0, y: 2.0, z: 3.0, crs: 'wgs-84-3d'})")))
  }

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

  test("point function should work with literal map and 3D cartesian coordinates") {
    val result = executeWith(pointConfig - Configs.Version3_1 - Configs.AllRulePlanners,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'cartesian-3D'}) as point",
      expectedDifferentResults = Configs.Version3_1 + Configs.AllRulePlanners,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.3, 4.5, 6.7))))
  }

  test("point function should work with literal map and srid") {
    val result = executeWith(pointConfig, "RETURN point({x: 2.3, y: 4.5, srid: 4326}) as point",
      expectedDifferentResults = Configs.Version3_1 + Configs.AllRulePlanners,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("point function should work with literal map and geographic coordinates") {
    val result = executeWith(pointConfig, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'WGS-84'}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
      expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 2.3, 4.5))))
  }

  test("point function should work with node with only valid properties") {
    val result = executeWith(pointConfig, "CREATE (n {latitude: 12.78, longitude: 56.7}) RETURN point(n) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should work with node with some invalid properties") {
    val result = executeWith(pointConfig, "CREATE (n {latitude: 12.78, longitude: 56.7, banana: 'yes', some: 1.2, andAlso: [1,2]}) RETURN point(n) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 56.7, 12.78))))
  }

  test("point function should not work with NaN or infinity") {
    for(invalidDouble <- Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)) {
      failWithError(Configs.DefaultInterpreted + Configs.SlottedInterpreted + Configs.Version3_3 + Configs.Procs,
        "RETURN point({x: 2.3, y: $v}) as point", List("Cannot create a point with non-finite coordinate values"), params = Map(("v", invalidDouble)))
    }
  }

  test("point function should not work with literal map and incorrect cartesian CRS") {
    failWithError(pointConfig + Configs.Procs,
      "RETURN point({x: 2.3, y: 4.5, crs: 'cart'}) as point", List("'cart' is not a supported coordinate reference system for points",
      "Unknown coordinate reference system: cart"))
  }

  test("point function should not work with literal map of 2 coordinates and incorrect cartesian-3D crs") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({x: 2.3, y: 4.5, crs: 'cartesian-3D'}) as point", List(
      "'cartesian-3D' is not a supported coordinate reference system for points",
      "Cannot create point with 3D coordinate reference system and 2 coordinates. Please consider using equivalent 2D coordinate reference system"))
  }

  test("point function should not work with literal map of 3 coordinates and incorrect cartesian crs") {
    failWithError(pointConfig - Configs.Version3_1 - Configs.AllRulePlanners + Configs.Procs,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'cartesian'}) as point",
      List("Cannot create point with 2D coordinate reference system and 3 coordinates. Please consider using equivalent 3D coordinate reference system"))
  }

  test("point function should not work with literal map and incorrect geographic CRS") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS84'}) as point",
      List("'WGS84' is not a supported coordinate reference system for points", "Unknown coordinate reference system: WGS84"))
  }

  test("point function should not work with literal map of 2 coordinates and incorrect WGS84-3D crs") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({x: 2.3, y: 4.5, crs: 'WGS-84-3D'}) as point", List(
      "'WGS-84-3D' is not a supported coordinate reference system for points",
      "Cannot create point with 3D coordinate reference system and 2 coordinates. Please consider using equivalent 2D coordinate reference system"))
  }

  test("point function should not work with literal map of 3 coordinates and incorrect WGS84 crs") {
    failWithError(pointConfig - Configs.Version3_1 - Configs.AllRulePlanners + Configs.Procs,
      "RETURN point({x: 2.3, y: 4.5, z: 6.7, crs: 'wgs-84'}) as point", List(
      "Cannot create point with 2D coordinate reference system and 3 coordinates. Please consider using equivalent 3D coordinate reference system"))
  }

  test("point function should work with integer arguments") {
    val result = executeWith(pointConfig, "RETURN point({x: 2, y: 4}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2, 4))))
  }

  // We can un-ignore this if/when we re-enable strict map checks in PointFunction.scala
  ignore("point function should throw on unrecognized map entry") {
    val stillWithoutFix = Configs.Version3_1 + Configs.AllRulePlanners
    failWithError(pointConfig - stillWithoutFix + Configs.Procs, "RETURN point({x: 2, y:3, a: 4}) as point", Seq("Unknown key 'a' for creating new point"))
  }

  test("should fail properly if missing cartesian coordinates") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({params}) as point",
      List("A cartesian point must contain 'x' and 'y'",
           "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" /* in version < 3.4 */),
      params = Map("params" -> Map("y" -> 1.0, "crs" -> "cartesian")))
  }

  test("should fail properly if missing geographic longitude") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({params}) as point",
      List("A wgs-84 point must contain 'latitude' and 'longitude'",
           "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" /* in version < 3.4 */),
      params = Map("params" -> Map("latitude" -> 1.0, "crs" -> "WGS-84")))
  }

  test("should fail properly if missing geographic latitude") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({params}) as point",
      List("A wgs-84 point must contain 'latitude' and 'longitude'",
           "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'" /* in version < 3.4 */),
      params = Map("params" -> Map("longitude" -> 1.0, "crs" -> "WGS-84")))
  }

  test("should fail properly if unknown coordinate system") {
    failWithError(pointConfig + Configs.Procs, "RETURN point({params}) as point", List("'WGS-1337' is not a supported coordinate reference system for points",
      "Unknown coordinate reference system: WGS-1337"),
      params = Map("params" -> Map("x" -> 1, "y" -> 2, "crs" -> "WGS-1337")))
  }

  test("should default to Cartesian if missing cartesian CRS") {
    val result = executeWith(pointConfig, "RETURN point({x: 2.3, y: 4.5}) as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 2.3, 4.5))))
  }

  test("point function with invalid coordinate types should give reasonable error") {
    failWithError(pointConfig + Configs.Procs,
      "return point({x: 'apa', y: 0, crs: 'cartesian'})", List("String is not a valid coordinate type.", "Cannot assign"))
  }

  test("point function with invalid crs types should give reasonable error") {
    failWithError(pointConfig + Configs.Procs,
      "return point({x: 0, y: 0, crs: 5})", List("java.lang.Long cannot be cast to", "java.lang.Long incompatible with", "Cannot assign"))
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
    failWithError(pointConfig + Configs.Procs, "RETURN point({longitude: 2.3, latitude: 4.5, crs: 'cartesian'}) as point",
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
    result.toList.length should be(1)
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("3D point should be assignable to node property") {
    // Given
    createLabeledNode("Place")

    // When
    val config = pointConfig - Configs.Cost3_1 - Configs.AllRulePlanners - Configs.Morsel
    val result = executeWith(config, "MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))))
  }

  test("3D point should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point")

    // When
    val result = executeWith(Configs.All, "MATCH (p:Place) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperatorWithText("Projection", "point"),
        expectPlansToFail = Configs.AllRulePlanners))

    // Then
    result.toList.length should be(1)
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/sr-org/9157/")
  }

  test("inequality on cartesian points") {
    // case same point
    shouldCompareLike("point({x: 0, y: 0})", "point({x: 0, y: 0})", aBiggerB = false, aSmallerB = false)

    // case top right quadrant
    shouldCompareLike("point({x: 1, y: 1})", "point({x: 0, y: 0})", aBiggerB = true, aSmallerB = false)
    // case bottom left quadrant
    shouldCompareLike("point({x: -1, y: -1})", "point({x: 0, y: 0})", aBiggerB = false, aSmallerB = true)
    // case top left quadrant
    shouldCompareLike("point({x: -1, y: 1})", "point({x: 0, y: 0})", aBiggerB = null, aSmallerB = null)
    // case bottom right quadrant
    shouldCompareLike("point({x: 1, y: -1})", "point({x: 0, y: 0})", aBiggerB = null, aSmallerB = null)

    // case straight top
    shouldCompareLike("point({x: 0, y: 1})", "point({x: 0, y: 0})", aBiggerB = true, aSmallerB = false)
    // case straight right
    shouldCompareLike("point({x: 1, y: 0})", "point({x: 0, y: 0})", aBiggerB = true, aSmallerB = false)
    // case straight bottom
    shouldCompareLike("point({x: 0, y: -1})", "point({x: 0, y: 0})", aBiggerB = false, aSmallerB = true)
    // case straight left
    shouldCompareLike("point({x: -1, y: 0})", "point({x: 0, y: 0})", aBiggerB = false, aSmallerB = true)
  }

  test("inequality on geographic points") {
    // case same point
    shouldCompareLike("point({longitude: 0, latitude: 0})", "point({longitude: 0, latitude: 0})", aBiggerB = false, aSmallerB = false)

    // case top right quadrant
    shouldCompareLike("point({longitude: 1, latitude: 1})", "point({longitude: 0, latitude: 0})", aBiggerB = true, aSmallerB = false)
    // case bottom left quadrant
    shouldCompareLike("point({longitude: -1, latitude: -1})", "point({longitude: 0, latitude: 0})", aBiggerB = false, aSmallerB = true)
    // case top left quadrant
    shouldCompareLike("point({longitude: -1, latitude: 1})", "point({longitude: 0, latitude: 0})", aBiggerB = null, aSmallerB = null)
    // case bottom right quadrant
    shouldCompareLike("point({longitude: 1, latitude: -1})", "point({longitude: 0, latitude: 0})", aBiggerB = null, aSmallerB = null)

    // case straight top
    shouldCompareLike("point({longitude: 0, latitude: 1})", "point({longitude: 0, latitude: 0})", aBiggerB = true, aSmallerB = false)
    // case straight right
    shouldCompareLike("point({longitude: 1, latitude: 0})", "point({longitude: 0, latitude: 0})", aBiggerB = true, aSmallerB = false)
    // case straight bottom
    shouldCompareLike("point({longitude: 0, latitude: -1})", "point({longitude: 0, latitude: 0})", aBiggerB = false, aSmallerB = true)
    // case straight left
    shouldCompareLike("point({longitude: -1, latitude: 0})", "point({longitude: 0, latitude: 0})", aBiggerB = false, aSmallerB = true)

    // the poles might be the same point, but in the effective projection onto 2D plane, they are not the same
    shouldCompareLike("point({longitude: -1, latitude: 90})", "point({longitude: 1, latitude: 90})", aBiggerB = false, aSmallerB = true)
    shouldCompareLike("point({longitude: 1, latitude: 90})", "point({longitude: -1, latitude: 90})", aBiggerB = true, aSmallerB = false)
    shouldCompareLike("point({longitude: -1, latitude: -90})", "point({longitude: 1, latitude: -90})", aBiggerB = false, aSmallerB = true)
    shouldCompareLike("point({longitude: 1, latitude: -90})", "point({longitude: -1, latitude: -90})", aBiggerB = true, aSmallerB = false)
  }

  test("inequality on 3D points") {
    Seq("cartesian-3D","WGS-84-3D").foreach { crsName =>
      (-1 to 1).foreach { x =>
        (-1 to 1).foreach { y =>
          (-1 to 1).foreach { z =>
            val same = x == 0 && y == 0 && z == 0
            val smaller = x <= 0 && y <= 0 && z <= 0
            val larger = x >= 0 && y >= 0 && z >= 0
            if (same) {
              shouldCompareLike(s"point({x: $x, y: $y, z: $z, crs: '$crsName'})", s"point({x: 0, y: 0, z: 0, crs: '$crsName'})", aBiggerB = false, aSmallerB = false)
            } else if (smaller) {
              shouldCompareLike(s"point({x: $x, y: $y, z: $z, crs: '$crsName'})", s"point({x: 0, y: 0, z: 0, crs: '$crsName'})", aBiggerB = false, aSmallerB = true)
            } else if (larger) {
              shouldCompareLike(s"point({x: $x, y: $y, z: $z, crs: '$crsName'})", s"point({x: 0, y: 0, z: 0, crs: '$crsName'})", aBiggerB = true, aSmallerB = false)
            } else {
              shouldCompareLike(s"point({x: $x, y: $y, z: $z, crs: '$crsName'})", s"point({x: 0, y: 0, z: 0, crs: '$crsName'})", aBiggerB = null, aSmallerB = null)
            }
          }
        }
      }
    }
  }

  test("inequality on mixed points") {
    shouldCompareLike("point({longitude: 0, latitude: 0})", "point({x: 0, y: 0})", aBiggerB = null, aSmallerB = null)
  }

  private def shouldCompareLike(a: String, b: String, aBiggerB: Any, aSmallerB: Any) = {
    val query =
      s"""WITH $a as a, $b as b
         |RETURN a > b, a < b
      """.stripMargin

    val pointConfig = Configs.Interpreted - Configs.BackwardsCompatibility - Configs.AllRulePlanners
    val result = executeWith(pointConfig, query).toList
    withClue(s"Comparing '$a' to '$b'") {
      result should equal(List(Map("a > b" -> aBiggerB, "a < b" -> aSmallerB)))
    }
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

  test("array of 3D cartesian points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute(
      """
        |UNWIND [1,2,3] as num
        |WITH point({x: num, y: num, z: num}) as p
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
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 2.0, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 3.0, 3.0, 3.0)
    ))
  }

  test("array of 3D WGS84 points should be readable from node property") {
    // Given
    createLabeledNode("Place")
    graph.execute(
      """
        |UNWIND [1,2,3] as num
        |WITH point({longitude: num, latitude: num, height: num}) as p
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
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 1.0, 1.0, 1.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 2.0, 2.0, 2.0),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 3.0, 3.0, 3.0)
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

  test("accessors on 2D cartesian points") {
    // given
    graph.execute("CREATE (:P {p : point({x: 1, y: 2})})")

    // when
    val result = executeWith(Configs.All - Configs.OldAndRule, "MATCH (n:P) WITH n.p AS p RETURN p.x, p.y, p.crs, p.srid")

    // then
    result.toList should be(List(Map("p.x" -> 1.0, "p.y" -> 2.0, "p.crs" -> "cartesian", "p.srid" -> 7203)))
    failWithError(Configs.All - Configs.OldAndRule + Configs.Procs, "MATCH (n:P) WITH n.p AS p RETURN p.latitude", Seq("Field: latitude is not available"))
    failWithError(Configs.All - Configs.OldAndRule + Configs.Procs, "MATCH (n:P) WITH n.p AS p RETURN p.z", Seq("Field: z is not available"))
  }

  test("accessors on 3D cartesian points") {
    // given
    graph.execute("CREATE (:P {p : point({x: 1, y: 2, z:3})})")

    // when
    val result = executeWith(Configs.All - Configs.OldAndRule, "MATCH (n:P) WITH n.p AS p RETURN p.x, p.y, p.z, p.crs, p.srid")

    // then
    result.toList should be(List(Map("p.x" -> 1.0, "p.y" -> 2.0, "p.z" -> 3.0, "p.crs" -> "cartesian-3d", "p.srid" -> 9157)))
    failWithError(Configs.All - Configs.OldAndRule + Configs.Procs, "MATCH (n:P) WITH n.p AS p RETURN p.latitude", Seq("Field: latitude is not available"))
  }

  test("accessors on 2D geographic points") {
    // given
    graph.execute("CREATE (:P {p : point({longitude: 1, latitude: 2})})")

    // when
    val result = executeWith(Configs.All - Configs.OldAndRule,  "MATCH (n:P) WITH n.p AS p RETURN p.longitude, p.latitude, p.crs, p.x, p.y, p.srid")

    // then
    result.toList should be(List(Map("p.longitude" -> 1.0, "p.latitude" -> 2.0, "p.crs" -> "wgs-84", "p.x" -> 1.0, "p.y" -> 2.0, "p.srid" -> 4326)))
    failWithError(Configs.All - Configs.OldAndRule + Configs.Procs, "MATCH (n:P) WITH n.p AS p RETURN p.height", Seq("Field: height is not available"))
    failWithError(Configs.All - Configs.OldAndRule + Configs.Procs, "MATCH (n:P) WITH n.p AS p RETURN p.z", Seq("Field: z is not available"))
  }

  test("accessors on 3D geographic points") {
    // given
    graph.execute("CREATE (:P {p : point({longitude: 1, latitude: 2, height:3})})")

    // when
    val result = executeWith(Configs.All - Configs.OldAndRule,
                             "MATCH (n:P) WITH n.p AS p RETURN p.longitude, p.latitude, p.height, p.crs, p.x, p.y, p.z, p.srid")

    // then
    result.toList should be(List(Map("p.longitude" -> 1.0, "p.latitude" -> 2.0, "p.height" -> 3.0, "p.crs" -> "wgs-84-3d", "p.srid" -> 4979,
                                     "p.x" -> 1.0, "p.y" -> 2.0, "p.z" -> 3.0)))
  }
}
