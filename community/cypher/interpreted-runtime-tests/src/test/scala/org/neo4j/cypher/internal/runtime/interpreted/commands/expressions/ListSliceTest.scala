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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST
import org.neo4j.values.virtual.VirtualValues.list

class ListSliceTest extends InterpretedRuntimeTestSuite {

  test("tests") {
    implicit val collection = literal(Seq(1, 2, 3, 4))

    slice(from = 0, to = 2) should equal(list(longValue(1), longValue(2)))
    slice(to = -2) should equal(list(longValue(1), longValue(2)))
    slice(from = 0, to = -1) should equal(list(longValue(1), longValue(2), longValue(3)))
    slice(from = 2) should equal(list(longValue(3), longValue(4)))
    slice(from = -3) should equal(list(longValue(2), longValue(3), longValue(4)))
    slice(to = 2) should equal(list(longValue(1), longValue(2)))
    slice(from = -3, to = -1) should equal(list(longValue(2), longValue(3)))
    sliceValue(null) should equal(Values.NO_VALUE)
  }

  test("should_handle_null") {
    implicit val collection = Literal(Values.NO_VALUE)

    slice(from = -3, to = -1) should equal(Values.NO_VALUE)
  }

  test("should_handle_out_of_bounds_by_returning_null") {
    val fullSeq = Seq(1, 2, 3, 4)
    implicit val collection = literal(fullSeq)

    slice(from = 2, to = 10) should equal(list(longValue(3), longValue(4)))
    slice(to = -10) should equal(EMPTY_LIST)
    slice(from = 5, to = -1) should equal(EMPTY_LIST)
    slice(from = 5) should equal(EMPTY_LIST)
    slice(from = -10) should equal(list(longValue(1), longValue(2), longValue(3), longValue(4)))
    slice(to = 10) should equal(list(longValue(1), longValue(2), longValue(3), longValue(4)))
    slice(from = -10, to = -1) should equal(list(longValue(1), longValue(2), longValue(3)))
  }

  test("should_handle_bounds_outside_int_range") {
    val fullSeq = Seq(1, 2, 3, 4)
    implicit val collection = literal(fullSeq)
    val full = list(longValue(1), longValue(2), longValue(3), longValue(4))
    val aboveMaxInt = Int.MaxValue.toLong + 1

    slice(from = 0, to = Int.MaxValue.toLong) should equal(full)
    slice(from = 0, to = aboveMaxInt) should equal(full)
    slice(to = aboveMaxInt) should equal(full)
    slice(from = aboveMaxInt) should equal(EMPTY_LIST)
    slice(from = aboveMaxInt, to = aboveMaxInt + 1) should equal(EMPTY_LIST)
    slice(from = -aboveMaxInt - 1) should equal(full)
    slice(to = -aboveMaxInt - 1) should equal(EMPTY_LIST)
    slice(from = -aboveMaxInt - 1, to = aboveMaxInt) should equal(full)
  }

  private val ctx = CypherRow.empty
  implicit private val state: QueryState = QueryStateHelper.empty
  private val NO_VALUE = -666L

  private def slice(from: Long = NO_VALUE, to: Long = NO_VALUE)(implicit collection: Expression) = {
    val f = if (from == NO_VALUE) None else Some(literal(from))
    val t = if (to == NO_VALUE) None else Some(literal(to))
    ListSlice(collection, f, t)(ctx, state)
  }

  private def sliceValue(in: Any) = {
    ListSlice(literal(in), Some(literal(0)), Some(literal(1)))(ctx, state)
  }
}
