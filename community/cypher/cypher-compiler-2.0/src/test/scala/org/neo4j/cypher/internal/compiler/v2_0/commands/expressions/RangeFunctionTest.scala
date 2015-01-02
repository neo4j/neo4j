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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryStateHelper
import org.neo4j.cypher.InvalidArgumentException

class RangeFunctionTest extends CypherFunSuite {

  test("range returns inclusive collection of integers") {
    range(0, 10, 1) should be(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    range(5, 12, 2) should be(Seq(5, 7, 9, 11))
    range(-3, 5, 1) should be(Seq(-3, -2, -1, 0, 1, 2, 3, 4, 5))
    range(-30, 50, 10) should be(Seq(-30, -20, -10, 0, 10, 20, 30, 40, 50))
  }

  test("range returns inclusive collection of integers for negative step values") {
    range(0, -10, -1) should be(Seq(0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10))
    range(-5, -12, -2) should be(Seq(-5, -7, -9, -11))
  }

  test("range throws when step is zero") {
    val thrown = intercept[InvalidArgumentException] {
      range(2, 8, 0)
    }
    assert(thrown.getMessage === "step argument to range() cannot be zero")
  }

  private def range(start: Int, end: Int, step: Int) = {
    val expr = RangeFunction(Literal(start), Literal(end), Literal(step))
    expr(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
