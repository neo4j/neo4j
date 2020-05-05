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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

abstract class ConditionalApplyTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("conditional apply should not run rhs if lhs is empty") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is true") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is false
    runtimeResult should beColumns("x").withSingleRow(null)
  }

  test("conditional apply on nonempty lhs and nonempty rhs") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
      nodeGraph(3, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.nodeByLabelScan("y", "RHS", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x").withRows(Seq(Array("42"), Array("42"), Array("42"), Array[Any](null), Array("43"), Array("43"), Array("43")))
  }
}
