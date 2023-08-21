/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.graphdb.spatial.Point
import org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN
import org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D
import org.neo4j.values.storable.Values.pointValue

import scala.util.Random

abstract class NodeIndexPointBoundingBoxSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should seek 2d cartesian points") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("location" -> pointValue(CARTESIAN, i, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.x AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        "{x: 2.0, y: 2.0, crs: 'cartesian'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2)))
  }

  test("should seek 3d cartesian points") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("location" -> pointValue(CARTESIAN_3D, i, 0, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.x AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, z: 0.0, crs: 'cartesian-3d'}",
        "{x: 2.0, y: 2.0, z: 2.0, crs: 'cartesian-3d'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2)))
  }

  test("should seek 2d geographic points") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        180,
        {
          case i => Map("location" -> pointValue(WGS_84, i % 180, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.longitude AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 0.0, latitude: 0.0, crs: 'wgs-84'}",
        "{longitude: 10.0, latitude: 0.0, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
  }

  test("should seek 3d geographic points") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        180,
        {
          case i => Map("location" -> pointValue(WGS_84_3D, i % 180, 0, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.longitude AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 0.0, latitude: 0.0, height: 0.0, crs: 'wgs-84-3d'}",
        "{longitude: 10.0, latitude: 0.0, height: 0.0, crs: 'wgs-84-3d'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
  }

  test("should cache properties") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("location" -> pointValue(CARTESIAN, i, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("cache[n.location] AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        "{x: 2.0, y: 2.0, crs: 'cartesian'}",
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location")
      .withRows(singleColumn(List(
        pointValue(CARTESIAN, 0, 0),
        pointValue(CARTESIAN, 1, 0),
        pointValue(CARTESIAN, 2, 0)
      )))
  }

  test("should handle bbox on the north-western hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 50, latitude: 50, crs: 'wgs-84'}",
        "{longitude: 60, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)

      (longitude >= 50 && longitude <= 60) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox on the north-eastern hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: -60, latitude: 50, crs: 'wgs-84'}",
        "{longitude: -50, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)

      (longitude >= -60 && longitude <= -50) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox on the south-western hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 50, latitude: -60, crs: 'wgs-84'}",
        "{longitude: 60, latitude: -50, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)

      (longitude >= 50 && longitude <= 60) && (latitude >= -60 && latitude <= -50)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox on the south-eastern hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: -60, latitude: -60, crs: 'wgs-84'}",
        "{longitude: -50, latitude: -50, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)

      (longitude >= -60 && longitude <= -50) && (latitude >= -60 && latitude <= -50)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox crossing the dateline") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 170, latitude: 50, crs: 'wgs-84'}",
        "{longitude: -170, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude >= 170 || longitude <= -170) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox crossing the equator") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        180,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 5, latitude: -10, crs: 'wgs-84'}",
        "{longitude: 10, latitude: 10, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (latitude >= -10 && latitude <= 10) && (longitude >= 5 && longitude <= 10)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox crossing the dateline and the equator") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 170, latitude: -10, crs: 'wgs-84'}",
        "{longitude: -170, latitude: 10, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude >= 170 || longitude <= -170) && (latitude >= -10 && latitude <= 10)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox with lowerLeft east of upperRight on the north-western hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 20, latitude: 50, crs: 'wgs-84'}",
        "{longitude: 10, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude >= 20 || longitude <= 10) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox with lowerLeft east of upperRight on the north-eastern hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: -10, latitude: 50, crs: 'wgs-84'}",
        "{longitude: -20, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude <= -20 || longitude >= -10) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox with lowerLeft east of upperRight on the south-western hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 20, latitude: -60, crs: 'wgs-84'}",
        "{longitude: 10, latitude: -50, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude >= 20 || longitude <= 10) && (latitude >= -60 && latitude <= -50)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox with lowerLeft east of upperRight on the south-eastern hemisphere") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: -10, latitude: -60, crs: 'wgs-84'}",
        "{longitude: -20, latitude: -50, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude <= -20 || longitude >= -10) && (latitude >= -60 && latitude <= -50)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should handle bbox crossing the dateline with lowerLeft east of upperRight") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: -170, latitude: 50, crs: 'wgs-84'}",
        "{longitude: 170, latitude: 60, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      (longitude <= 170 && longitude >= -170) && (latitude >= 50 && latitude <= 60)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("bbox with lowerLeft north of upperRight is empty") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        180,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            Map("location" -> pointValue(WGS_84, longitude, latitude))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 10, latitude: 50, crs: 'wgs-84'}",
        "{longitude: 20, latitude: 40, crs: 'wgs-84'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should handle 3D bbox") {
    val nodes = given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        sizeHint,
        {
          case _ =>
            val longitude = 180 - Random.nextInt(361)
            val latitude = 90 - Random.nextInt(181)
            val height = Random.nextInt(1000)
            Map("location" -> pointValue(WGS_84_3D, longitude, latitude, height))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{longitude: 50, latitude: 50, height: 100, crs: 'wgs-84-3d'}",
        "{longitude: 60, latitude: 60, height: 200, crs: 'wgs-84-3d'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = nodes.filter(n => {
      val coordinate = n.getProperty("location").asInstanceOf[Point].getCoordinate.getCoordinate
      val longitude = coordinate(0)
      val latitude = coordinate(1)
      val height = coordinate(2)

      (longitude >= 50 && longitude <= 60) &&
      (latitude >= 50 && latitude <= 60) &&
      (height >= 100 && height <= 200)
    })
    runtimeResult should beColumns("n").withRows(singleColumn(expected))
  }

  test("should ignore non-points and points with different CRS") {
    given {
      nodeIndex(IndexType.POINT, "Place", "location")
      nodePropertyGraph(
        100,
        {
          case i => Map("location" -> i)
        },
        "Place"
      )
      nodePropertyGraph(
        100,
        {
          case i => Map("location" -> pointValue(WGS_84, i, 0))
        },
        "Place"
      )
      nodePropertyGraph(
        100,
        {
          case i => Map("location" -> pointValue(WGS_84_3D, i, 0, 0))
        },
        "Place"
      )
      nodePropertyGraph(
        100,
        {
          case i => Map("location" -> pointValue(CARTESIAN_3D, i, 0, 0))
        },
        "Place"
      )
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("location" -> pointValue(CARTESIAN, i, 0))
        },
        "Place"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.x AS location")
      .pointBoundingBoxNodeIndexSeekExpr(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        "{x: 2.0, y: 2.0, crs: 'cartesian'}",
        indexType = IndexType.POINT
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2)))
  }
}
