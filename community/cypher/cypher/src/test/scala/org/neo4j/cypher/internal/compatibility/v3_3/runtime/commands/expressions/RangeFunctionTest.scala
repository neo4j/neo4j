/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.list

class RangeFunctionTest extends CypherFunSuite {

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

  private def seq(vals: Long*) = list(vals.map(Values.longValue):_*)

  private def range(start: Long, end: Long, step: Long): ListValue = {
    val expr = RangeFunction(Literal(start), Literal(end), Literal(step))
    expr(ExecutionContext.empty, QueryStateHelper.empty).asInstanceOf[ListValue]
  }
}
