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

import java.io.File
import java.util.stream.Collectors

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.io.fs.FileUtils
import org.neo4j.values.storable.{CoordinateReferenceSystem, PointValue, Values}

class SpatialIndexAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  private var dbDir = new File("test")

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
    createLabeledNode("Place")

    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("persisted indexed 3D point should be readable from node property") {
    graph.createIndex("Place", "location")
    createLabeledNode("Place")

    graph.execute("MATCH (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0}) RETURN p.location as point")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0))
  }

  test("persisted indexed 3D cartesian point should be readable from node property") {
    graph.createIndex("Place", "location")
    createLabeledNode("Place")

    graph.execute("MATCH (p:Place) SET p.location = point({x: -1, y: 1, z: -1}) RETURN p.location as point")

    testPointRead("MATCH (p:Place) WHERE p.location = point({x: -1, y: 1, z: -1}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -1, 1, -1))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({x: -1, y: 1, z: -1}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, -1, 1, -1))
  }

  test("create index after adding node and also survive restart") {
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    graph.createIndex("Place", "location")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    restartGraphDatabase()

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("indexScan should handle multiple different types of points and also survive restart") {
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, crs: 'cartesian'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0, crs: 'WGS-84-3D'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, z: 5.0, crs: 'cartesian-3D'})")

    val query = "MATCH (p:Place) WHERE EXISTS(p.location) RETURN p.location AS point"
    testPointScan(query,
      Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))

    restartGraphDatabase()

    testPointScan(query,
      Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7),
      Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78),
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))
  }

  test("indexScan should handle multiple different types of 3D points and also survive restart") {
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0, crs: 'WGS-84-3D'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, z: 5.0, crs: 'cartesian-3D'})")

    val query = "MATCH (p:Place) WHERE EXISTS(p.location) RETURN p.location AS point"
    testPointScan(query,
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))

    restartGraphDatabase()

    testPointScan(query,
      Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0),
      Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))
  }

  test("indexSeek should handle multiple different types of points and also survive restart") {
    graph.createIndex("Place", "location")

    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, crs: 'cartesian'})")
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0, crs: 'WGS-84-3D'})")
    graph.execute("CREATE (p:Place) SET p.location = point({x: 1.0, y: 2.78, z: 5.0, crs: 'cartesian-3D'})")

    val query1 = "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location AS point"
    val query2 = "MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78, crs: 'cartesian'}) RETURN p.location AS point"
    val query3 = "MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, height: 100.0, crs: 'WGS-84-3D'}) RETURN p.location AS point"
    val query4 = "MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78, z: 5.0, crs: 'cartesian-3D'}) RETURN p.location AS point"

    testPointRead(query1, Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    testPointRead(query2, Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78))
    testPointRead(query3, Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0))
    testPointRead(query4, Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))

    restartGraphDatabase()

    testPointRead(query1, Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
    testPointRead(query2, Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78))
    testPointRead(query3, Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0))
    testPointRead(query4, Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0))
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

  test("create drop create index") {
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point")
    graph.createIndex("Place", "location")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    graph.execute("DROP INDEX ON :Place(location)")
    graph.createIndex("Place", "location")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, crs: 'WGS-84'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  test("change crs") {
    graph.execute("CREATE (p:Place) SET p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location as point")
    graph.createIndex("Place", "location")

    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))

    // When changing to Cartesian
    graph.execute("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78}) SET p.location = point({x: 1.0, y: 2.78})")
    testPointRead("MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78, crs: 'cartesian'}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78))

    // When changing to Cartesian-3D
    graph.execute("MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78}) SET p.location = point({x: 1.0, y: 2.78, z: 3.2})")
    testPointRead("MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78, z: 3.2}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 3.2))

    // When changing to WGS84-3D
    graph.execute("MATCH (p:Place) WHERE p.location = point({x: 1.0, y: 2.78, z: 3.2}) SET p.location = point({latitude: 56.7, longitude: 12.78, height: 123.0})")
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, height: 123.0}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 123.0))

    // When changing back to WGS84-3D
    graph.execute("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78, height: 123.0}) SET p.location = point({latitude: 56.7, longitude: 12.78})")
    testPointRead("MATCH (p:Place) WHERE p.location = point({latitude: 56.7, longitude: 12.78}) RETURN p.location as point", Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7))
  }

  private def testPointRead(query: String, expected: PointValue*): Unit = {
    testPointScanOrRead(query, seek = true, expected)
  }

  private def testPointScan(query: String, expected: PointValue*): Unit = {
    testPointScanOrRead(query, seek = false, expected)
  }

  private def testPointScanOrRead(query: String, seek: Boolean, expected: Seq[PointValue]): Unit = {
    val result = graph.execute(query)

    val plan = result.getExecutionPlanDescription.toString
    if (seek) plan should include("NodeIndexSeek")
    else plan should include("NodeIndexScan")
    plan should include (":Place(location)")

    val points = result.columnAs("point").stream().collect(Collectors.toSet)
    expected.foreach(p => assert(points.contains(p)))
    points.size() should be(expected.size)
  }

}
