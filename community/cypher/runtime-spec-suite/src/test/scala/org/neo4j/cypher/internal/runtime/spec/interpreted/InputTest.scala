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
package org.neo4j.cypher.internal.runtime.spec.interpreted

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.runtime.spec._

abstract class InputTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should produce input") {
    // given
    val nodes = nodeGraph(3)

    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x", "y", "z")
      .input("x", "y", "z")
      .build()

    val input =
      inputValues(
        Array(11, 12, 13),
        Array(21, 22, 23),
        Array(31, 32, 33))
      .and(
        Array("11", "12", "13"),
        nodes.toArray
      )

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(input.flatten)
  }

  test("should retain input value order") {
    // when
    val columns = (0 until 100).map(i => "c"+i)

    val logicalQuery = new LogicalQueryBuilder()
      .produceResults(columns:_*)
      .input(columns:_*)
      .build()

    val input = inputValues(columns.toArray)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns(columns:_*).withRow(columns:_*)
  }

  test("should return no rows on no input") {
    // when
    val logicalQuery = new LogicalQueryBuilder()
      .produceResults("x", "y", "z")
      .input("x", "y", "z")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)

    // then
    runtimeResult should beColumns("x", "y", "z").withNoRows()
  }
}

class InterpretedInputTest extends InputTestBase(COMMUNITY_EDITION, InterpretedRuntime)
