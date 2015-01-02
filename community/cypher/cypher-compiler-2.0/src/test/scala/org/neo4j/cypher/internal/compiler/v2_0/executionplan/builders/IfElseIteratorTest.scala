/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext

class IfElseIteratorTest extends Assertions {
  val startIterator = Iterator(ExecutionContext.empty)
  val a = ExecutionContext.from("a" -> 1)
  val b = ExecutionContext.from("a" -> 2)

  @Test def should_pass_through_if_ifClause_returns_values() {
    val ifClause = (_: ExecutionContext) => Iterator(a)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(startIterator, ifClause, elseClause, () => {})
    assert(result.toList === List(a))
  }

  @Test def should_return_elseClause_value_if_ifClause_is_empty() {
    val ifClause = (_: ExecutionContext) => Iterator()
    val elseClause = (_: ExecutionContext) => Iterator(b)

    val result = new IfElseIterator(startIterator, ifClause, elseClause, () => {})
    assert(result.toList === List(b))
  }

  @Test def should_return_all_values_produces_by_ifClause() {
    val ifClause = (_: ExecutionContext) => Iterator(a, b)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(startIterator, ifClause, elseClause, () => {})
    assert(result.toList === List(a, b))
  }

  @Test def should_be_empty_when_the_input_is_empty() {
    val ifClause = (_: ExecutionContext) => fail("should not have run")
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(Iterator.empty, ifClause, elseClause, () => {})
    assert(result.toList === List())
  }
  
  @Test def should_run_finally_block_when_if_succeeds() {
    var touched = false
    val ifClause = (_: ExecutionContext) => Iterator(a)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(startIterator, ifClause, elseClause, () => touched = true)
    assert(result.toList === List(a))
    assert(touched, "The finally block was never run")
  }
  
  @Test def should_run_finally_block_when_if_fails() {
    var touched = false
    val ifClause = (_: ExecutionContext) => Iterator(a)
    val elseClause = (_: ExecutionContext) => fail("should not have run")

    val result = new IfElseIterator(startIterator, ifClause, elseClause, () => touched = true)
    assert(result.toList === List(a))
    assert(touched, "The finally block was never run")
  }
}
