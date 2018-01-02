/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class IfElseIteratorTest extends CypherFunSuite {
  val a1 = ExecutionContext.from("a" -> 1)
  val a2 = ExecutionContext.from("a" -> 2)

  test("should_pass_through_if_ifClause_returns_values") {
    val ifClause = (_: ExecutionContext) => Iterator(a1)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator(ExecutionContext.empty), ifClause, elseClause, () => {})
    result.toList should equal(List(a1))
  }

  test("should_return_elseClause_value_if_ifClause_is_empty") {
    val ifClause = (_: ExecutionContext) => Iterator()
    val elseClause = (_: ExecutionContext) => Iterator(a2)

    val result = new IfElseIterator(Iterator(ExecutionContext.empty), ifClause, elseClause, () => {})
    result.toList should equal(List(a2))
  }

  test("should_return_all_values_produces_by_ifClause") {
    val ifClause = (_: ExecutionContext) => Iterator(a1, a2)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator(ExecutionContext.empty), ifClause, elseClause, () => {})
    result.toList should equal(List(a1, a2))
  }

  test("should_be_empty_when_the_input_is_empty") {
    val ifClause = (_: ExecutionContext) => fail("should not have run")
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator.empty, ifClause, elseClause, () => {})
    result.toList should equal(List())
  }

  test("should_run_finally_block_when_if_succeeds") {
    var touched = false
    val ifClause = (_: ExecutionContext) => Iterator(a1)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator(ExecutionContext.empty), ifClause, elseClause, () => touched = true)
    result.toList should equal(List(a1))
    touched should equal(true)
  }

  test("should_run_finally_block_when_if_fails") {
    var touched = false
    val ifClause = (_: ExecutionContext) => Iterator(a1)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator(ExecutionContext.empty), ifClause, elseClause, () => touched = true)
    result.toList should equal(List(a1))
    touched should equal(true)
  }
}
