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

import java.io.File
import java.util.stream.Collectors

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.spatial.Point
import org.neo4j.io.fs.FileUtils
import org.neo4j.values.storable.{CoordinateReferenceSystem, PointValue, Values}

import scala.collection.Map

class SpatialIndexAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  var dbDir = new File("test")

  override protected def initTest() {
    FileUtils.deleteRecursively(dbDir)
    startGraphDatabase(dbDir)
  }

  override protected def startGraphDatabase(storeDir: File): Unit = {
    graphOps = graphDatabaseFactory().newEmbeddedDatabase(storeDir)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  protected def restartGraphDatabase(): Unit = {
    graph.shutdown()
    startGraphDatabase(dbDir)
  }

  override protected def stopTest() {
    try {
      super.stopTest()
    }
    finally {
      if (graph != null) graph.shutdown()
      FileUtils.deleteRecursively(dbDir)
    }
  }

  test("persisted indexed point should be readable from node property") {
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("create index after adding node and also survive restart") {
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    graph.createIndex("Place", "location")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("should handle multiple different types of points and also survive restart") {
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, crs: 'cartesian'})")

    val query = "MATCH (p:Place) WHERE EXISTS(p.location) RETURN p.location AS point"
    testPointScan(query, Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7), Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78))

    restartGraphDatabase()

    testPointScan(query, Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7), Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78))
  }

  test("overwriting indexed property should work") {
    graph.createIndex("Place", "location")
    createLabeledNode("Place")

    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point")
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.0, 56.0))

    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("create index before and after adding node and also survive restart") {
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.0, 56.0))
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.0, longitude: 12.0, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.0, 56.0))
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  private def testPointRead(query: String, expected: PointValue): Unit = {

    val result = graph.execute(query)

    val plan = result.getExecutionPlanDescription.toString
    plan should include ("NodeIndexSeek")
    plan should include (":Place(location)")
    // Then
    val point = result.columnAs("point").next().asInstanceOf[Point]
    point should equal(expected)
    // And CRS names should equal
    point.getCRS.getHref should equal("http://spatialreference.org/ref/epsg/4326/")
  }

  private def testPointScan(query: String, expected: PointValue*): Unit = {
    val result = graph.execute(query)

    val plan = result.getExecutionPlanDescription.toString
    plan should include ("NodeIndexScan")
    plan should include (":Place(location)")

    val points = result.columnAs("point").stream().collect(Collectors.toSet)
    expected.foreach(p => assert(points.contains(p)))
    points.size() should be(expected.size)
  }
}
