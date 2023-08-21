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
import org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN
import org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84
import org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D
import org.neo4j.values.storable.Values.pointValue

abstract class NodeIndexPointDistanceSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should seek 2d cartesian points (inclusively)") {
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
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        2,
        indexType = IndexType.POINT,
        inclusive = true
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2)))
  }

  test("should seek 2d cartesian points (non-inclusively)") {
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
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        2,
        indexType = IndexType.POINT,
        inclusive = false
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1)))
  }

  test("should seek 3d cartesian points (inclusively)") {
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
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, z: 0.0, crs: 'cartesian-3d'}",
        2,
        indexType = IndexType.POINT,
        inclusive = true
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1, 2)))
  }

  test("should seek 3d cartesian points (non-inclusively)") {
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
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, z: 0.0, crs: 'cartesian-3d'}",
        2,
        indexType = IndexType.POINT,
        inclusive = false
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("location").withRows(singleColumn(List(0, 1)))
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
    val d = WGS_84.getCalculator.distance(pointValue(WGS_84, 0, 0), pointValue(WGS_84, 10, 0))
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.longitude AS location")
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{longitude: 0.0, latitude: 0.0, crs: 'wgs-84'}",
        d,
        indexType = IndexType.POINT,
        inclusive = true
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
    val d = WGS_84_3D.getCalculator.distance(pointValue(WGS_84_3D, 0, 0, 0), pointValue(WGS_84_3D, 10, 0, 0))
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("location")
      .projection("n.location.longitude AS location")
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{longitude: 0.0, latitude: 0.0, height: 0.0, crs: 'wgs-84-3d'}",
        d,
        indexType = IndexType.POINT,
        inclusive = true
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
      .pointDistanceNodeIndexSeek(
        "n",
        "Place",
        "location",
        "{x: 0.0, y: 0.0, crs: 'cartesian'}",
        2,
        indexType = IndexType.POINT,
        inclusive = true,
        getValue = GetValue
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
}
