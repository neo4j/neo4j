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

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.io.fs.FileUtils
import org.neo4j.values.storable.{CoordinateReferenceSystem, Values}

class SpatialIndexAcceptanceTest extends IndexingTestSupport {

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

  private val wgs1 = Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7)
  private val wgs2 = Values.pointValue(CoordinateReferenceSystem.WGS84, 44.4, 44.5)
  private val wgs1_3d = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0)
  private val car = Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78)
  private val car_3d = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0)

  test("persisted indexed point should be seekable from node property") {
    createIndex()
    val node = createIndexedNode(wgs1)

    assertSeekMatchFor(wgs1, node)

    restartGraphDatabase()

    assertSeekMatchFor(wgs1, node)
  }

  test("different types of indexed points should survive restart") {
    createIndex()

    val n1 = createIndexedNode(wgs1)
    val n2 = createIndexedNode(wgs1_3d)
    val n3 = createIndexedNode(car)
    val n4 = createIndexedNode(car_3d)

    assertScanMatch(n1, n2, n3, n4)

    restartGraphDatabase()

    assertScanMatch(n1, n2, n3, n4)
  }

  test("overwriting indexed property should work") {
    createIndex()
    val node = createIndexedNode(wgs1)
    assertSeekMatchFor(wgs1, node)

    setIndexedValue(node, wgs2)
    assertSeekMatchFor(wgs2, node)

    restartGraphDatabase()

    assertSeekMatchFor(wgs2, node)
  }

  test("create index before and after adding node and also survive restart") {
    val n1 = createIndexedNode(wgs1)
    createIndex()
    val n2 = createIndexedNode(wgs2)

    assertSeekMatchFor(wgs1, n1)
    assertSeekMatchFor(wgs2, n2)

    restartGraphDatabase()

    assertSeekMatchFor(wgs1, n1)
    assertSeekMatchFor(wgs2, n2)
  }

  test("create drop create index") {
    val n1 = createIndexedNode(wgs1)
    createIndex()

    assertSeekMatchFor(wgs1, n1)

    dropIndex()
    createIndex()

    assertSeekMatchFor(wgs1, n1)
  }

  test("change crs") {
    val n1 = createIndexedNode(wgs1)
    createIndex()

    assertSeekMatchFor(wgs1, n1)

    // When changing to Cartesian
    setIndexedValue(n1, car)
    assertSeekMatchFor(car, n1)

    // When changing to Cartesian-3D
    setIndexedValue(n1, car_3d)
    assertSeekMatchFor(car_3d, n1)

    // When changing to WGS84-3D
    setIndexedValue(n1, wgs1_3d)
    assertSeekMatchFor(wgs1_3d, n1)

    // When changing back to WGS84
    setIndexedValue(n1, wgs1)
    assertSeekMatchFor(wgs1, n1)
  }
}
