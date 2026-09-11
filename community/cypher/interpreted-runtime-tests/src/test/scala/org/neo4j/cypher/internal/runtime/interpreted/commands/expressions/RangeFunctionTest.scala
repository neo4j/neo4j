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
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.list

class RangeFunctionTest extends InterpretedRuntimeTestSuite {

  test("range returns inclusive collection of integers") {
    range(0, 10, 1) should be(seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    range(5, 12, 2) should be(seq(5, 7, 9, 11))
    range(-3, 5, 1) should be(seq(-3, -2, -1, 0, 1, 2, 3, 4, 5))
    range(-30, 50, 10) should be(seq(-30, -20, -10, 0, 10, 20, 30, 40, 50))
  }

  test("range returns inclusive collection of integers for negative step values") {
    range(0, -10, -1) should be(seq(0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10))
    range(-5, -12, -2) should be(seq(-5, -7, -9, -11))
  }

  test("range should not overflow when more than 32bits") {
    range(2147483647L, 2147483648L, 1L) should be(seq(2147483647L, 2147483648L))
  }

  test("should work on ranges having length bigger than Int.MaxValue") {
    range(1L, Int.MaxValue + 1000L, 1L) // should not blow up...
  }

  test("should compute exact cardinality beyond Int.MaxValue (#13957)") {
    range(0L, Int.MaxValue.toLong, 1L).actualSize() should be(2147483648L)
    range(Int.MinValue.toLong, Int.MaxValue.toLong, 1L).actualSize() should be(4294967296L)
    range(Int.MinValue.toLong, Int.MaxValue.toLong, 2L).actualSize() should be(2147483648L)
    range(Int.MaxValue.toLong, Int.MinValue.toLong, -1L).actualSize() should be(4294967296L)
    // long indexing stays valid without materialization
    range(0L, 4294967296L, 1L).value(4294967296L) should be(Values.longValue(4294967296L))
    // intSize is exact-or-fail, never wraps to 0
    an[org.neo4j.exceptions.ArithmeticException] should be thrownBy range(0L, Int.MaxValue.toLong, 1L).intSize()
  }

  test("should fail materialization of huge range before allocation (#13957)") {
    // 2^32 would historically narrow to 0, 2^32+1 to 1, 2^32+2 to 2
    an[RuntimeException] should be thrownBy range(0L, 4294967295L, 1L).toStorableArray()
    an[RuntimeException] should be thrownBy range(0L, 4294967296L, 1L).toStorableArray()
    an[RuntimeException] should be thrownBy range(0L, 4294967297L, 1L).toStorableArray()
  }

  private def seq(vals: Long*) = list(vals.map(Values.longValue): _*)

  private def range(start: Long, end: Long, step: Long): ListValue = {
    val expr = RangeFunction(literal(start), literal(end), literal(step))
    expr(CypherRow.empty, QueryStateHelper.empty).asInstanceOf[ListValue]
  }
}
