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

import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.values.storable.{CoordinateReferenceSystem, PointValue, Values}

import scala.collection.Map
import scala.collection.immutable.{Map => ImmutableMap}

class SpatialIndexResultsAcceptanceTest extends IndexingTestSupport {

  private val equalityConfig = Configs.Interpreted - Configs.OldAndRule
  private val indexConfig = Configs.Interpreted - Configs.BackwardsCompatibility - Configs.AllRulePlanners

  override def cypherComparisonSupport = true

  test("inequality query should give same answer for indexed and non-indexed property") {
    createIndex()
    val point = Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 0)
    val node = createIndexedNode(point)
    setNonIndexedValue(node, point)

    assertRangeScanFor("<", point)
    assertLabelRangeScanFor("<", point)

    assertRangeScanFor("<=", point, node)
    assertLabelRangeScanFor("<=", point, node)

    assertRangeScanFor("<", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 0), node)
    assertLabelRangeScanFor("<", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 0), node)
  }

  test("indexed point should be readable from node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // When
    val localConfig = Configs.Interpreted - Configs.Version2_3 - Configs.AllRulePlanners
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point should be readable from parameterized node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // When
    val localConfig = Configs.All - Configs.OldAndRule
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3),
      params = ImmutableMap("param" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7)))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point array of size 1 should be readable from parameterized node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = [point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})] RETURN p.location as point")

    // When
    val localConfig = Configs.All - Configs.OldAndRule
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3),
      params = ImmutableMap("param" -> Array(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7)))
    // And CRS names should equal
    pointList.head.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point array should be readable from parameterized node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute(
      """MATCH (p:Place) SET p.location =
        |[point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}),
        | point({y: 56.7, x: 13.78, crs: 'WGS-84'})]
        |RETURN p.location as point""".stripMargin)

    // When
    val localConfig = Configs.All - Configs.OldAndRule
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3),
      params = ImmutableMap("param" ->
        Array(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
          Values.pointValue(CoordinateReferenceSystem.WGS84, 13.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(
      Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 13.78, 56.7))
    )

    // And CRS names should equal
    pointList.head.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point array should be readable from parameterized (as list) node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute(
      """MATCH (p:Place) SET p.location =
        |[point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}),
        | point({y: 56.7, x: 13.78, crs: 'WGS-84'})]
        |RETURN p.location as point""".stripMargin)

    // When
    val localConfig = Configs.All - Configs.OldAndRule
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3),
      params = ImmutableMap("param" ->
        List(Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
          Values.pointValue(CoordinateReferenceSystem.WGS84, 13.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(
      Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
      Values.pointValue(CoordinateReferenceSystem.WGS84, 13.78, 56.7))
    )

    // And CRS names should equal
    pointList.head.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("seeks should work for indexed point arrays") {
    createIndex()

    val point1 = Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 3.4).asObjectCopy()
    val point2 = Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2, 5.6).asObjectCopy()

    val pointArray1 = Values.pointArray(Array(point1, point2))
    val pointArray2 = Values.pointArray(Array(point2, point1))
    val pointArray3 = Values.pointArray(Array(point1, point2, point1))

    val n1 = createIndexedNode(pointArray1)
    createIndexedNode(pointArray2)
    createIndexedNode(pointArray3)

    assertSeekMatchFor(pointArray1, n1)
  }

  test("with multiple indexed points only exact match should be returned") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 40.7, longitude: -35.78, crs: 'WGS-84'})")

    val configuration = TestConfiguration(Versions(Versions.V3_3, Versions.V3_4, Versions.Default), Planners(Planners.Cost, Planners.Default), Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.Default))
    // When
    val result = executeWith(configuration,
      "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - configuration))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("3D indexed point should be readable from node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point")

    // When
    val result = executeWith(Configs.Interpreted - Configs.Version3_1 - Configs.Version2_3 - Configs.AllRulePlanners,
      "MATCH (p:Place) WHERE p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - Configs.Version3_4 - Configs.Version3_3))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/sr-org/9157/")
  }

  test("with multiple 3D indexed points only exact match should be returned") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.601})")

    val configuration = TestConfiguration(Versions(Versions.V3_3, Versions.V3_4, Versions.Default), Planners(Planners.Cost, Planners.Default), Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.Default))
    // When
    val result = executeWith(configuration,
      "MATCH (p:Place) WHERE p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should useOperatorWithText("Projection", "point")
        plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
      }, expectPlansToFail = Configs.AbsolutelyAll - configuration))

    // Then
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.2, 3.4, 5.6))))
  }

  test("indexed points far apart in cartesian space - range query greaterThan") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: -100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: -100000})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location > point({x: 0, y: 0}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) > point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 100000, 100000))))
  }

  test("indexed points far apart in cartesian space - range query lessThan") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: -100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: -100000})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location < point({x: 0, y: 0}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) < point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, -100000, -100000))))
  }

  test("indexed points far apart in cartesian space - range query within") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 500000, y: 500000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: 100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: -100000, y: -100000})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 100000, y: -100000})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location > point({x: 0, y: 0}) AND p.location < point({x: 200000, y: 200000}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) > point", ":Place(location) < point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.Cartesian, 100000, 100000))))
  }

  test("indexed points far apart in WGS84 - range query greaterThan") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 5.7, longitude: 116.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: -50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: -10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location > point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) > point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("indexed points close together in WGS84 - equality query") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.700001, longitude: 12.7800001, crs: 'WGS-84'})")

    // When
    val result = executeWith(equalityConfig, "CYPHER MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("Index query with MERGE") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.700001, longitude: 12.7800001, crs: 'WGS-84'})")

    // When matching in merge
    val result = executeWith(equalityConfig, "MERGE (p:Place {location: point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) }) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("NodeIndexSeek", ":Place(location)")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))

    //  And when creating in merge
    val result2 = executeWith(equalityConfig, "MERGE (p:Place {location: point({latitude: 156.7, longitude: 112.78, crs: 'WGS-84'}) }) RETURN p.location as point")

    // Then
    val plan2 = result2.executionPlanDescription()
    plan2 should useOperatorWithText("NodeIndexSeek", ":Place(location)")
    result2.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 112.78, 156.7))))
  }

  test("indexed points close together in WGS84 - range query greaterThan") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location > point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) > point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("indexed points close together in WGS84 - range query greaterThanOrEqualTo") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location >= point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) >= point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("indexed points close together in WGS84 - range query greaterThan with no results") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location > point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) > point")
    assert(result.isEmpty)
  }

  test("indexed points close together in WGS84 - range query greaterThan with multiple CRS") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({y: 56.7, x: 12.78, crs: 'cartesian'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location >= point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) >= point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))))
  }

  test("indexed points close together in WGS84 - range query within") {
    // Given
    graph.createIndex("Place", "location")
    graph.execute("CREATE (p:Place) SET p.location = point({y: 55.7, x: 11.78, crs: 'cartesian'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 50.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 10.78, crs: 'WGS-84'})")

    // When
    val result = executeWith(indexConfig, "CYPHER MATCH (p:Place) WHERE p.location >= point({latitude: 55.7, longitude: 11.78, crs: 'WGS-84'}) AND p.location < point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // Then
    val plan = result.executionPlanDescription()
    plan should useOperatorWithText("Projection", "point")
    plan should useOperatorWithText("NodeIndexSeekByRange", ":Place(location) >= point", ":Place(location) < point")
    result.toList should equal(List(Map("point" -> Values.pointValue(CoordinateReferenceSystem.WGS84, 11.78, 55.7))))
  }

  test("indexed points in 3D cartesian space - range queries") {
    // Given
    createIndex()
    val originPoint = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 0, 0, 0)
    val maxPoint = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 100000, 100000, 100000)
    val minPoint = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -100000, -100000, -100000)

    val origin = createIndexedNode(originPoint)
    val upRightTop = createIndexedNode(maxPoint)
    val downLeftBottom = createIndexedNode(minPoint)

    val downrightTop = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -100000, 100000, 100000))
    val downLeftTop = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -100000, -100000, 100000))
    val upLeftTop = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 100000, -100000, 100000))
    val upRightBottom = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 100000, 100000, -100000))
    val downRightBottom = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -100000, 100000, -100000))
    val upLeftBottom = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 100000, -100000, -100000))
    // 2D points should never be returned
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, -100000, -100000))
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 100000, 100000))

    assertRangeScanFor(">", originPoint, upRightTop)
    assertRangeScanFor(">=", originPoint, origin, upRightTop)
    assertRangeScanFor("<", originPoint, downLeftBottom)
    assertRangeScanFor("<=", originPoint, origin, downLeftBottom)
    assertRangeScanFor(">", minPoint, "<", maxPoint, origin, downLeftTop, downRightBottom, downrightTop, upLeftBottom, upLeftTop, upRightBottom)
  }

  test("indexed points 3D WGS84 space - range queries") {
    // Given
    createIndex()
    val maxPoint = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.6, 13.1, 100)
    val midPoint = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.5, 13.0, 50)
    val minPoint = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.4, 12.9, 0)

    val n0 = createIndexedNode(midPoint)
    val n1 = createIndexedNode(maxPoint)
    val n2 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.4, 13.1, 100))
    val n3 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.4, 12.9, 100))
    val n4 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.6, 12.9, 0))
    val n5 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.6, 13.1, 0))
    val n6 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.4, 13.1, 0))
    val n8 = createIndexedNode(minPoint)
    val n7 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 56.6, 12.9, 0))
    // 2D points should never be returned
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84, 56.6, 13.1))
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.WGS84, 56.4, 12.9))

    assertRangeScanFor(">", midPoint, n1)
    assertRangeScanFor(">=", midPoint, n0, n1)
    assertRangeScanFor("<", midPoint, n8)
    assertRangeScanFor("<=", midPoint, n0, n8)
    assertRangeScanFor(">", minPoint, "<", maxPoint, n0, n2, n3, n4, n5, n6, n7)
  }
}
