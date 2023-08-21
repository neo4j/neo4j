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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class PartialTop1TestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(1, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("simple sorting works as expected") {
    // when
    val input = inputValues(
      Array("A", 2),
      Array("A", 1),
      Array("B", 0)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(1, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y")
      .withSingleRow("A", 1)
  }

  test("two ties for the first place are not all returned") {
    // when
    val input = inputValues(
      Array(1, 5),
      Array(1, 5),
      Array(1, 2),
      Array(1, 2),
      Array(2, 4),
      Array(2, 3),
      Array(2, 0)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(1, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y")
      .withSingleRow(1, 2)
  }

  test("if only null is present, it should be returned") {
    // when
    val input = inputValues(
      Array(null, null),
      Array(null, null),
      Array(null, 2),
      Array(null, 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(1, Seq("x ASC"), Seq("y DESC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y")
      .withSingleRow(null, null)
  }

}
