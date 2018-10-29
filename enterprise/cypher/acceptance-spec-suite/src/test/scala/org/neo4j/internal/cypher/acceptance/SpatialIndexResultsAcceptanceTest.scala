/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Values

import scala.collection.Map
import scala.collection.immutable.{Map => ImmutableMap}

class SpatialIndexResultsAcceptanceTest extends IndexingTestSupport {

  private val equalityConfig = Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.Version3_1
  private val indexConfig = Configs.InterpretedAndSlotted - Configs.Cost3_1 - Configs.Cost2_3 - Configs.RulePlanner

  override def cypherComparisonSupport = true

  test("inequality query should give same answer for indexed and non-indexed property") {
    createIndex()
    val point = cartesianPoint(0, 0)
    val node = createIndexedNode(point)
    setNonIndexedValue(node, point)

    assertRangeScanFor("<", point)
    assertLabelRangeScanFor("<", point)

    assertRangeScanFor("<=", point, node)
    assertLabelRangeScanFor("<=", point, node)

    assertRangeScanFor("<", cartesianPoint(1, 1), node)
    assertLabelRangeScanFor("<", cartesianPoint(1, 1), node)

    assertRangeScanFor("<=", cartesianPoint(1, 0), node)
    assertLabelRangeScanFor("<=", cartesianPoint(1, 0), node)
  }

  test("indexed point should be readable from node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // When
    val localConfig = Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.RulePlanner
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(wgsPoint(12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point should be readable from parameterized node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    // When
    val localConfig = Configs.All - Configs.Version2_3 - Configs.Version3_1
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))

      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4),
      params = ImmutableMap("param" -> wgsPoint(12.78, 56.7)))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(wgsPoint(12.78, 56.7))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("indexed point array of size 1 should be readable from parameterized node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = [point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})] RETURN p.location as point")

    // When
    val localConfig = Configs.All - Configs.Version2_3 - Configs.Version3_1
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4),
      params = ImmutableMap("param" -> Array(wgsPoint(12.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(wgsPoint(12.78, 56.7)))
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
    val localConfig = Configs.All - Configs.Version2_3 - Configs.Version3_1
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4),
      params = ImmutableMap("param" ->
        Array(wgsPoint(12.78, 56.7),
          wgsPoint(13.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(
      wgsPoint(12.78, 56.7),
      wgsPoint(13.78, 56.7))
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
    val localConfig = Configs.All - Configs.Version2_3 - Configs.Version3_1
    val result = executeWith(localConfig,
      "MATCH (p:Place) WHERE p.location = $param RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4),
      params = ImmutableMap("param" ->
        List(wgsPoint(12.78, 56.7),
          wgsPoint(13.78, 56.7))))

    // Then
    val pointList = result.columnAs("point").toList.head.asInstanceOf[Iterable[PointValue]].toList
    pointList should equal(List(
      wgsPoint(12.78, 56.7),
      wgsPoint(13.78, 56.7))
    )

    // And CRS names should equal
    pointList.head.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  test("seeks should work for indexed point arrays") {
    createIndex()

    val point1 = cartesianPoint(1.2, 3.4).asObjectCopy()
    val point2 = cartesianPoint(1.2, 5.6).asObjectCopy()

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

    val configuration = TestConfiguration(Versions(Versions.V3_4, Versions.V3_5), Planners.Cost,
                                          Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.SlottedWithCompiledExpressions))
    val query = "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point"

    // When
    val result = executeWith(configuration + Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - configuration))

    // Then
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
  }

  test("3D indexed point should be readable from node property") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point")

    // When
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (p:Place) WHERE p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - Configs.Version3_5 - Configs.Version3_4))

    // Then
    val point = result.columnAs("point").toList.head.asInstanceOf[Point]
    point should equal(cartesianPoint(1.2, 3.4, 5.6))
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/sr-org/9157/")
  }

  test("with multiple 3D indexed points only exact match should be returned") {
    // Given
    graph.createIndex("Place", "location")
    createLabeledNode("Place")
    graph.execute("MATCH (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.2, y: 3.4, z: 5.601})")

    val configuration = TestConfiguration(Versions(Versions.V3_4, Versions.V3_5),
                                          Planners(Planners.Cost),
                                          Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.SlottedWithCompiledExpressions))
    // When
    val result = executeWith(configuration,
      "MATCH (p:Place) WHERE p.location = point({x: 1.2, y: 3.4, z: 5.6}) RETURN p.location as point",
      planComparisonStrategy = ComparePlansWithAssertion({ plan =>
        plan should includeSomewhere
          .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
          .onTopOf(aPlan("NodeIndexSeek").containingArgument(":Place(location)"))
      }, expectPlansToFail = Configs.All - configuration))

    // Then
    result.toList should equal(List(Map("point" -> cartesianPoint(1.2, 3.4, 5.6))))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) > point.*".r))
    result.toList should equal(List(Map("point" -> cartesianPoint(100000, 100000))))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) < point.*".r))
    result.toList should equal(List(Map("point" -> cartesianPoint(-100000, -100000))))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) > point.* AND :Place\\(location\\) < point.*".r))
    result.toList should equal(List(Map("point" -> cartesianPoint(100000, 100000))))
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

    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) > point.*".r))
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
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
    plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":Place(location)")
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
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
    plan should includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":Place(location)")
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))

    //  And when creating in merge
    val result2 = executeWith(equalityConfig, "MERGE (p:Place {location: point({latitude: 156.7, longitude: 112.78, crs: 'WGS-84'}) }) RETURN p.location as point")

    // Then
    val plan2 = result2.executionPlanDescription()
    plan2 should includeSomewhere.aPlan("NodeIndexSeek").containingArgument(":Place(location)")
    result2.toList should equal(List(Map("point" -> wgsPoint(112.78, 156.7))))
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

    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) > point.*".r))
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
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

    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) >= point.*".r))
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) > point.*".r))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) >= point.*".r))
    result.toList should equal(List(Map("point" -> wgsPoint(12.78, 56.7))))
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
    plan should includeSomewhere
      .aPlan("Projection").containingArgumentRegex("\\{point : .*\\}".r)
      .onTopOf(aPlan("NodeIndexSeekByRange").containingArgumentRegex(":Place\\(location\\) >= point.* AND :Place\\(location\\) < point.*".r))
    result.toList should equal(List(Map("point" -> wgsPoint(11.78, 55.7))))
  }

  test("indexed points in 3D cartesian space - range queries") {
    // Given
    createIndex()
    val originPoint = cartesianPoint(0, 0, 0)
    val maxPoint = cartesianPoint(100000, 100000, 100000)
    val minPoint = cartesianPoint(-100000, -100000, -100000)

    val origin = createIndexedNode(originPoint)
    val upRightTop = createIndexedNode(maxPoint)
    val downLeftBottom = createIndexedNode(minPoint)

    val downrightTop = createIndexedNode(cartesianPoint(-100000, 100000, 100000))
    val downLeftTop = createIndexedNode(cartesianPoint(-100000, -100000, 100000))
    val upLeftTop = createIndexedNode(cartesianPoint(100000, -100000, 100000))
    val upRightBottom = createIndexedNode(cartesianPoint(100000, 100000, -100000))
    val downRightBottom = createIndexedNode(cartesianPoint(-100000, 100000, -100000))
    val upLeftBottom = createIndexedNode(cartesianPoint(100000, -100000, -100000))
    // 2D points should never be returned
    createIndexedNode(cartesianPoint(-100000, -100000))
    createIndexedNode(cartesianPoint(100000, 100000))

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
    val maxPoint = wgsPoint(56.6, 13.1, 100)
    val midPoint = wgsPoint(56.5, 13.0, 50)
    val minPoint = wgsPoint(56.4, 12.9, 0)

    val n0 = createIndexedNode(midPoint)
    val n1 = createIndexedNode(maxPoint)
    val n2 = createIndexedNode(wgsPoint(56.4, 13.1, 100))
    val n3 = createIndexedNode(wgsPoint(56.4, 12.9, 100))
    val n4 = createIndexedNode(wgsPoint(56.6, 12.9, 0))
    val n5 = createIndexedNode(wgsPoint(56.6, 13.1, 0))
    val n6 = createIndexedNode(wgsPoint(56.4, 13.1, 0))
    val n8 = createIndexedNode(minPoint)
    val n7 = createIndexedNode(wgsPoint(56.6, 12.9, 0))
    // 2D points should never be returned
    createIndexedNode(wgsPoint(56.6, 13.1))
    createIndexedNode(wgsPoint(56.4, 12.9))

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
    val nodeAbove15 = createIndexedNode(cartesianPoint(1.2345, 5.4321))
    val nodeBelow15 = createIndexedNode(cartesianPoint(0.2345, 4.4321))
    val nodeAt15 = createIndexedNode(cartesianPoint(1, 5))
    val nodeAt25 = createIndexedNode(cartesianPoint(2, 5))
    val nodeAt16 = createIndexedNode(cartesianPoint(1, 6))
    val nodeAt05 = createIndexedNode(cartesianPoint(0, 5))
    val nodeAt14 = createIndexedNode(cartesianPoint(1, 4))
    createIndexedNode(cartesianPoint(0.2345, 5.4321))
    createIndexedNode(cartesianPoint(1.2345, 4.4321))

    Map(
      ">=" -> Set(nodeAbove15, nodeAt15, nodeAt16, nodeAt25),
      "<=" -> Set(nodeBelow15, nodeAt15, nodeAt05, nodeAt14),
      ">" -> Set(nodeAbove15),
      "<" -> Set(nodeBelow15)
    ).toList.foreach {
      case (op, expected) =>
        // When
        val resultsWithIndex = executeSingle(s"MATCH (n:Label) WHERE n.prop $op point({x: 1, y: 5}) RETURN n").toList.map(_ ("n"))
        val resultsWithoutIndex = executeSingle(s"MATCH (n) WHERE n.prop $op point({x: 1, y: 5}) RETURN n").toList.map(_ ("n"))

        // Then
        val expectAxes = op contains "="
        withClue(s"Should ${if (expectAxes) "" else "NOT "}find nodes that are on the axes defined by the search point when using operator '$op' and index") {
          resultsWithIndex.toSet should be(expected)
        }

        // And should get same results without an index
        withClue(s"Should ${if (expectAxes) "" else "NOT "}find nodes that are on the axes defined by the search point when using operator '$op' and no index") {
          resultsWithoutIndex.toSet should be(expected)
        }
    }
    // When running a spatial range query for points greater than or equal to the search point
    val includingBorder = executeSingle("MATCH (n:Label) WHERE n.prop >= point({x: 1, y: 5}) RETURN n")

    // Then expect to also find nodes on the intersecting axes
    withClue("Should find nodes that are on the axes defined by the search point") {
      includingBorder.toList.map(_ ("n")).toSet should be(Set(nodeAbove15, nodeAt15, nodeAt16, nodeAt25))
    }

    // When running a spatial range query for points only greater than the search point
    val excludingBorder = executeSingle("MATCH (n:Label) WHERE n.prop > point({x: 1, y: 5}) RETURN n")

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
        createIndexedNode(cartesianPoint(x, y))
      }
    }
    val nodeToFind = createIndexedNode(cartesianPoint(1.2345, 5.4321))
    createIndexedNode(cartesianPoint(5.4321, 1.2345))
    val vertices = Seq(grid(11)(15), grid(11)(16), grid(12)(16), grid(12)(15))

    // When running a bounding box query we expect to include all or none of the border points
    val includingBorder = executeSingle("MATCH (n:Label) WHERE point({x: 1, y: 5}) <= n.prop <= point({x: 2, y: 6}) RETURN n")
    withClue("Should find nodes that are on the axes defined by the search point") {
      includingBorder.toList.size should be(5)
    }
    val excludingBorder = executeSingle("MATCH (n:Label) WHERE point({x: 1, y: 5}) < n.prop < point({x: 2, y: 6}) RETURN n")
    withClue("Should find nodes that are on NOT the axes defined by the search point") {
      excludingBorder.toList.size should be(1)
    }

    // And when using the range scan assertions we should find the same results
    val minPoint = cartesianPoint(1, 5)
    val maxPoint = cartesianPoint(2, 6)
    assertRangeScanFor(">=", minPoint, "<=", maxPoint, vertices :+ nodeToFind: _*)
    assertRangeScanFor(">", minPoint, "<", maxPoint, nodeToFind)
  }

  test("should not return points on edges even when using transaction state") {
    val atPoint = wgsPoint(1, 5)
    val onAxisX = wgsPoint(1, 4)
    val onAxisY = wgsPoint(0, 5)
    val inRange = wgsPoint(0, 4)
    val query = "MATCH (n:Label) WHERE n.prop < {prop} RETURN n, n.prop AS prop ORDER BY id(n)"
    createIndex()
    createIndexedNode(atPoint)

    def runTest(name: String): Unit = {
      val results = executeSingle(query, scala.Predef.Map("prop" -> atPoint))
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

  test("should use index if predicate depends on property of variable from horizon") {
    createIndex()
    createLabeledNode(Map("location" -> cartesianPoint(5, 5)), "MIN")
    createLabeledNode(Map("location" -> cartesianPoint(10, 10)), "MAX")
    createIndexedNode(cartesianPoint(5, 5))
    val n1 = createIndexedNode(cartesianPoint(5.1, 9.9))
    val n2 = createIndexedNode(cartesianPoint(9.9, 5.1))
    createIndexedNode(cartesianPoint(10, 10))
    val expected = Seq(n1, n2)

    val query =
      s"""
         |MATCH (a:MIN), (b:MAX)
         |WITH a.location AS min, b.location AS max
         |MATCH (p:$LABEL) USING INDEX SEEK p:$LABEL($PROPERTY)
         |WHERE min < p.$PROPERTY < max
         |RETURN p
      """.stripMargin

    val result =
      executeWith(
        Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.Version3_1 - Configs.Version3_4,
        query,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange")
          .containingArgument(s":$LABEL($PROPERTY) > min AND :$LABEL($PROPERTY) < max"))
      )
    val nodes = result.columnAs[Node]("p").toSet
    expected.foreach(p => assert(nodes.contains(p)))
    nodes.size should be(expected.size)
  }

  test("should plan index usage if predicate depends on simple variable from horizon") {
    createIndex()
    createLabeledNode(Map("location" -> cartesianPoint(5, 5)), "MIN")
    createLabeledNode(Map("location" -> cartesianPoint(10, 10)), "MAX")
    createIndexedNode(cartesianPoint(5, 5))
    val n1 = createIndexedNode(cartesianPoint(5.1, 9.9))
    val n2 = createIndexedNode(cartesianPoint(9.9, 5.1))
    createIndexedNode(cartesianPoint(10, 10))
    val expected = Seq(n1, n2)

    val query =
      s"""
         |WITH point({x:5, y:5}) as min, point({x:10, y:10}) as max
         |MATCH (p:$LABEL) USING INDEX SEEK p:$LABEL($PROPERTY)
         |WHERE point({x:min.x, y:min.y}) < p.$PROPERTY < max
         |RETURN p
      """.stripMargin

    val result =
      executeWith(
        Configs.InterpretedAndSlotted - Configs.Version2_3 - Configs.Version3_1 - Configs.Version3_4,
        query,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange")
          .containingArgument(s":$LABEL($PROPERTY) > point({x: min.x, y: min.y}) AND :$LABEL($PROPERTY) < max"))
      )
    val nodes = result.columnAs[Node]("p").toSet
    expected.foreach(p => assert(nodes.contains(p)))
    nodes.size should be(expected.size)
  }

  private def cartesianPoint(x: Double, y: Double) = Values.pointValue(CoordinateReferenceSystem.Cartesian, x, y)

  private def cartesianPoint(x: Double, y: Double, z: Double) = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, x, y, z)

  private def wgsPoint(longitude: Double, latitude: Double) = Values.pointValue(CoordinateReferenceSystem.WGS84, longitude, latitude)

  private def wgsPoint(longitude: Double, latitude: Double, z: Double) = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, longitude, latitude, z)
}
