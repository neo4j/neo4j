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
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.virtual.VirtualValues

abstract class SimulatedPlansTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should simulate node scan") {
    // given
    val n = sizeHint

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .simulatedNodeScan("x", n)
      .build()

    val runtimeResult = executeWithoutValuePopulation(logicalQuery, runtime, NoInput, Map.empty)

    // then
    val expected = (0 until n).map {
      i => Array(VirtualValues.node(i))
    }

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should simulate expand 1:1 and provide variables for relationship and end node") {
    // given
    val n = sizeHint

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .simulatedFilter(1.0)
      .simulatedExpand("x", "r", "y", 1.0)
      .simulatedNodeScan("x", n)
      .build()

    val runtimeResult = executeWithoutValuePopulation(logicalQuery, runtime, NoInput, Map.empty)

    // then
    val expected = (0 until n).map {
      i => Array(VirtualValues.node(i), VirtualValues.relationship(i), VirtualValues.node(i))
    }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should simulate expand 1:4 and provide variables for relationship and end node") {
    // given
    val n = sizeHint

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .simulatedFilter(1.0)
      .simulatedExpand("x", "r", "y", 4.0)
      .simulatedNodeScan("x", n)
      .build()

    val runtimeResult = executeWithoutValuePopulation(logicalQuery, runtime, NoInput, Map.empty)

    // then
    val expected = for {
      i <- (0 until n)
      _ <- (0 until 4)
    } yield {
      Array(VirtualValues.node(i), VirtualValues.relationship(i), VirtualValues.node(i))
    }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should simulate expand 1:3 and filter away everything") {
    // given
    val n = sizeHint

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .simulatedFilter(0.0)
      .simulatedExpand("x", "r", "y", 4.0)
      .simulatedNodeScan("x", n)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }
}
