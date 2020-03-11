/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Collections

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.collection.JavaConverters.seqAsJavaListConverter

abstract class RollupApplyTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                            runtime: CypherRuntime[CONTEXT],
                                                            val sizeHint: Int
                                                           ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty lhs should produce no rows") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollupApply("list", "y")
      .|.unwind("[1,2,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "list").withNoRows()
  }

  test("empty rhs should produce empty lists") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollupApply("list", "y")
      .|.allNodeScan("y", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val expectedRows: Iterable[Array[_]] = (0 until sizeHint).map { i =>
      Array[Any](i.toLong, Collections.emptyList())
    }
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x", "list").withRows(expectedRows)
  }

  test("non-empty lhs, non-empty rhs should produce lists of rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollupApply("list", "y")
      .|.unwind("[x, x] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val expectedRows: Iterable[Array[_]] = (0L until sizeHint).map { i =>
      Array[Any](i, java.util.List.of(i, i))
    }
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x", "list").withRows(expectedRows)
  }

  test("non-empty lhs, non-empty rhs should produce null lists for null lhs variables") {
    val size = Math.sqrt(sizeHint).toInt
    val (aNodes, bNodes) =
      given {
        bipartiteGraph(size, "A", "B", "R")
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollupApply("list", "y", Set("y"))
      .|.argument("y")
      .optionalExpandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    // then
    val expectedRows: Iterable[Array[_]] =
      aNodes.flatMap(a => bNodes.map(b => Array[Any](a, Collections.singletonList(b)))) ++
      bNodes.map(b => Array[Any](b, null))
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "list").withRows(expectedRows)
  }
}
