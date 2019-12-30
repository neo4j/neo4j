/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class CrossApplyTestBase[CONTEXT <: RuntimeContext](
                                                              edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int
                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("cross apply on empty lhs and rhs") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .crossApply()
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("cross apply on empty rhs") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .crossApply()
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("cross apply on empty lhs") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .crossApply()
      .|.nodeByLabelScan("y", "RHS", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("cross apply on aggregation") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xMax")
      .crossApply()
      .|.aggregation(Seq.empty, Seq("max(x.prop) as xMax"))
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "xMax").withRows(nodes.zipWithIndex.map(_.productIterator.toArray))
  }

  test("cross apply union with aliased variables") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("z")
      .crossApply()
      .|.distinct("z AS z")
      .|.union()
      .|.|.projection("y AS z")
      .|.|.argument("y")
      .|.projection("x AS z")
      .|.argument("x")
      .projection("1 AS x", "2 AS y")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("z").withRows(Array(Array(1), Array(2)))
  }

}
