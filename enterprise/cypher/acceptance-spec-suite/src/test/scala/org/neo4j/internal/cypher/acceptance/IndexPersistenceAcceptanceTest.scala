/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.File
import java.time.ZoneOffset

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.configuration.Settings
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings
import org.neo4j.values.storable._

import scala.collection.{Map, immutable}

class IndexPersistenceAcceptanceTest extends IndexingTestSupport {

  private var dbDir = new File("test")

  override val cypherComparisonSupport = false

  override protected def initTest() {
    FileUtils.deleteRecursively(dbDir)
    startGraphDatabase(dbDir)
  }

  override protected def startGraphDatabase(storeDir: File): Unit = {
    startGraphDatabaseWithConfig(storeDir, databaseConfig())
  }

  private def startGraphDatabaseWithConfig(storeDir: File, config: Map[Setting[_], String]): Unit = {
    val builder = graphDatabaseFactory().newEmbeddedDatabaseBuilder(storeDir)
    config.foreach {
      case (setting, settingValue) => builder.setConfig(setting, settingValue)
    }
    graphOps = builder.newGraphDatabase()
    graph = new GraphDatabaseCypherService(graphOps)
  }

  private def restartGraphDatabase(config: Map[Setting[_], String] = databaseConfig()): Unit = {
    graph.shutdown()
    startGraphDatabaseWithConfig(dbDir, config)
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
  private val date = DateValue.epochDate( 1000 )
  private val dateTime = DateTimeValue.datetime( 1000, 100, ZoneOffset.UTC )
  private val localDateTime = LocalDateTimeValue.localDateTime( 1000, 100 )
  private val time = TimeValue.time( 1000, ZoneOffset.UTC )
  private val localTime = LocalTimeValue.localTime( 1000 )
  private val duration = DurationValue.duration( 1, 2, 3, 4 )

  private val values: Array[Value] = Array(wgs1, wgs2, wgs1_3d, car, car_3d, date, dateTime, localDateTime, time, localTime, duration)

  test("persisted indexed property should be seekable from node property") {
    createIndex()
    val node = createIndexedNode(wgs1)

    assertSeekMatchFor(wgs1, node)

    restartGraphDatabase()

    assertSeekMatchFor(wgs1, node)
  }

  test("different types of indexed property should survive restart") {
    createIndex()

    val nodes = values.map(createIndexedNode)

    assertScanMatch(nodes:_*)

    restartGraphDatabase()

    assertScanMatch(nodes:_*)
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

  test("change value of indexed node") {
    val n1 = createIndexedNode(wgs1)
    createIndex()

    assertSeekMatchFor(wgs1, n1)

    for ( value <- values ) {
      setIndexedValue(n1, value)
      assertSeekMatchFor(value, n1)
    }
  }

  test("Should not get new index configuration on database settings changes of maxBits") {
    // halve the value of maxBits
    testIndexRestartWithSettingsChanges(Map(SpatialIndexSettings.space_filling_curve_max_bits -> "30"))
  }

  test("Should not get new index configuration on database settings changes of WGS84 minimum x extent") {
    // remove the entire western hemisphere
    val wgs84_x_min = SpatialIndexSettings.makeCRSRangeSetting(CoordinateReferenceSystem.WGS84, 0, "min")
    testIndexRestartWithSettingsChanges(Map(wgs84_x_min -> "0"))
  }

  private def testIndexRestartWithSettingsChanges(settings: Map[Setting[_], String]): Unit = {
    createIndex()
    val data = (-180 to 180 by 10).flatMap { lon =>
      (-90 to 90 by 10).map { lat =>
        val point = Values.pointValue(CoordinateReferenceSystem.WGS84, lon, lat)
        point -> createIndexedNode(point)
      }
    }.toMap
    val expected = Values.pointValue(CoordinateReferenceSystem.WGS84, 10, 50)
    val node = data(expected)
    val min = Values.pointValue(CoordinateReferenceSystem.WGS84, 1, 41)
    val max = Values.pointValue(CoordinateReferenceSystem.WGS84, 19, 59)

    assertRangeScanFor(">", min, "<", max, node)

    restartGraphDatabase(settings)

    assertRangeScanFor(">", min, "<", max, node)

    dropIndex()
    createIndex()

    assertRangeScanFor(">", min, "<", max, node)
  }
}
