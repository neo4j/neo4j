/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

    assertRangeScanFor("<", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 1), node)
    assertLabelRangeScanFor("<", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 1), node)

    assertRangeScanFor("<=", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 0), node)
    assertLabelRangeScanFor("<=", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 0), node)
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
    assertRangeScanFor(">=", minPoint, "<=", maxPoint, origin, upRightTop, downLeftBottom, downLeftTop, downRightBottom, downrightTop, upLeftBottom, upLeftTop, upRightBottom)
    assertRangeScanFor(">", minPoint, "<=", maxPoint, origin, upRightTop)
    assertRangeScanFor(">=", minPoint, "<", maxPoint, origin, downLeftBottom)
    assertRangeScanFor(">", minPoint, "<", maxPoint, origin)
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
    assertRangeScanFor(">=", minPoint, "<=", maxPoint, n0, n1, n2, n3, n4, n5, n6, n7, n8)
    assertRangeScanFor(">", minPoint, "<=", maxPoint, n0, n1)
    assertRangeScanFor(">=", minPoint, "<", maxPoint, n0, n8)
    assertRangeScanFor(">", minPoint, "<", maxPoint, n0)
  }

  test("Range query should return points greater on both axes or less on both axes") {
    // Given nodes at the search point, above and below it, and on the axes intersecting it
    createIndex()
    val nodeAbove15 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2345, 5.4321))
    val nodeBelow15 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 0.2345, 4.4321))
    val nodeAt15 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 5))
    val nodeAt25 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 2, 5))
    val nodeAt16 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 6))
    val nodeAt05 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 0, 5))
    val nodeAt14 = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 4))
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 0.2345, 5.4321))
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2345, 4.4321))

    Map(
      ">=" -> Set(nodeAbove15, nodeAt15, nodeAt16, nodeAt25),
      "<=" -> Set(nodeBelow15, nodeAt15, nodeAt05, nodeAt14),
      ">" -> Set(nodeAbove15),
      "<" -> Set(nodeBelow15)
    ).toList.foreach {
      case (op, expected) =>
        // When
        val resultsWithIndex = innerExecuteDeprecated(s"MATCH (n:Label) WHERE n.prop $op point({x: 1, y: 5}) RETURN n").toList.map(_ ("n"))
        val resultsWithoutIndex = innerExecuteDeprecated(s"MATCH (n) WHERE n.prop $op point({x: 1, y: 5}) RETURN n").toList.map(_ ("n"))

        // Then
        val expectAxes = op contains "="
        withClue(s"Should ${if(expectAxes) "" else "NOT "}find nodes that are on the axes defined by the search point when using operator '$op' and index") {
          resultsWithIndex.toSet should be(expected)
        }

        // And should get same results without an index
        withClue(s"Should ${if(expectAxes) "" else "NOT "}find nodes that are on the axes defined by the search point when using operator '$op' and no index") {
          resultsWithoutIndex.toSet should be(expected)
        }
    }
    // When running a spatial range query for points greater than or equal to the search point
    val includingBorder = innerExecuteDeprecated("MATCH (n:Label) WHERE n.prop >= point({x: 1, y: 5}) RETURN n")

    // Then expect to also find nodes on the intersecting axes
    withClue("Should find nodes that are on the axes defined by the search point") {
      includingBorder.toList.map(_ ("n")).toSet should be(Set(nodeAbove15, nodeAt15, nodeAt16, nodeAt25))
    }

    // When running a spatial range query for points only greater than the search point
    val excludingBorder = innerExecuteDeprecated("MATCH (n:Label) WHERE n.prop > point({x: 1, y: 5}) RETURN n")

    // Then expect to find nodes above the search point and not on the intersecting axes
    withClue("Should NOT find nodes that are on the axes defined by the search point") {
      excludingBorder.toList.map(_ ("n")).toSet should be(Set(nodeAbove15))
    }
  }

  test("Bounding box query on regular grid should not return points on the edges") {
    // Given
    createIndex()
    val grid = Range(-10, 10).map { x =>
      Range(-10, 10).map { y =>
        createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, x, y))
      }
    }
    val nodeToFind = createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.2345, 5.4321))
    createIndexedNode(Values.pointValue(CoordinateReferenceSystem.Cartesian, 5.4321, 1.2345))
    val vertices = Seq(grid(11)(15), grid(11)(16), grid(12)(16), grid(12)(15))

    // When running a bounding box query we expect to include all or none of the border points
    val includingBorder = innerExecuteDeprecated("MATCH (n:Label) WHERE point({x: 1, y: 5}) <= n.prop <= point({x: 2, y: 6}) RETURN n")
    withClue("Should find nodes that are on the axes defined by the search point") {
      includingBorder.toList.size should be(5)
    }
    val excludingBorder = innerExecuteDeprecated("MATCH (n:Label) WHERE point({x: 1, y: 5}) < n.prop < point({x: 2, y: 6}) RETURN n")
    withClue("Should find nodes that are on NOT the axes defined by the search point") {
      excludingBorder.toList.size should be(1)
    }

    // And when using the range scan assertions we should find the same results
    val minPoint = Values.pointValue(CoordinateReferenceSystem.Cartesian, 1, 5)
    val maxPoint = Values.pointValue(CoordinateReferenceSystem.Cartesian, 2, 6)
    assertRangeScanFor(">=", minPoint, "<=", maxPoint, vertices :+ nodeToFind: _*)
    assertRangeScanFor(">", minPoint, "<", maxPoint, nodeToFind)
  }

  test("should not return points on edges even when using transaction state") {
    val atPoint = Values.pointValue(CoordinateReferenceSystem.WGS84, 1, 5)
    val onAxisX = Values.pointValue(CoordinateReferenceSystem.WGS84, 1, 4)
    val onAxisY = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 5)
    val inRange = Values.pointValue(CoordinateReferenceSystem.WGS84, 0, 4)
    val query = "MATCH (n:Label) WHERE n.prop < {prop} RETURN n, n.prop AS prop ORDER BY id(n)"
    createIndex()
    createIndexedNode(atPoint)

    def runTest(name: String): Unit = {
      val results = innerExecuteDeprecated(query, scala.Predef.Map("prop" -> atPoint))
      withClue(s"Should not find on-axis points when searching for < $atPoint ($name)") {
        results.toList.map(_ ("prop")).toSet should be(Set(inRange))
      }
    }

    graph.inTx {
      Seq(onAxisX, onAxisY, inRange).foreach(createIndexedNode(_))
      runTest("nodes in same transaction")
    }
    runTest("no transaction state")
  }
}
