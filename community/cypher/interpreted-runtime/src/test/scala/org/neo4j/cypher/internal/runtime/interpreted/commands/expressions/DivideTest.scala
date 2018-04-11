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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.util.v3_4.ArithmeticException
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class DivideTest extends CypherFunSuite {
  test("should_throw_arithmetic_exception_for_divide_by_zero") {
    val ctx = ExecutionContext.empty
    val state = QueryStateHelper.empty

    intercept[ArithmeticException](Divide(Literal(1), Literal(0))(ctx, state))
    intercept[ArithmeticException](Divide(Literal(1.4), Literal(0))(ctx, state))
    intercept[ArithmeticException](Divide(Literal(1), Literal(0.0))(ctx, state))
    intercept[ArithmeticException](Divide(Literal(3.4), Literal(0.0))(ctx, state))
  }
}
