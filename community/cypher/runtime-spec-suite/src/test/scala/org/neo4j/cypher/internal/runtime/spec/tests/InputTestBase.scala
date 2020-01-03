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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class InputTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should produce input") {
    // given
    val nodes = given {
      nodeGraph(3)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .input(variables = Seq("x", "y", "z"))
      .build()

    val resultInput = inputValues(
      Array(11, 12, 13),
      Array(21, 22, 23),
      Array(31, 32, 33))
      .and(
        Array("11", "12", "13"),
        nodes.map(n => tx.getNodeById(n.getId)).toArray
      )

    val runtimeResult = execute(logicalQuery, runtime, resultInput.stream())

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(resultInput.flatten)
  }

  test("should retain input value order") {
    // when
    val columns = (0 until 100).map(i => "c"+i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns:_*)
      .input(variables = columns)
      .build()

    val input = inputValues(columns.toArray)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns(columns:_*).withSingleRow(columns:_*)
  }

  test("should return no rows on no input") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .input(variables = Seq("x", "y", "z"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    runtimeResult should beColumns("x", "y", "z").withNoRows()
  }
}
