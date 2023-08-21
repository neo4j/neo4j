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
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.ArithmeticException
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.NumberValue

class DivideTest extends CypherFunSuite {

  test("should_throw_arithmetic_exception_for_divide_by_zero") {
    val ctx = CypherRow.empty
    val state = QueryStateHelper.empty

    intercept[ArithmeticException](Divide(literal(1), literal(0))(ctx, state))
    intercept[ArithmeticException](Divide(literal(1.4), literal(0))(ctx, state))
    // Floating point division should not throw "/ by zero".
    // The JVM does not trap IEEE-754 exceptional conditions (see https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.8.1)
    // The behaviour is defined as:
    Divide(literal(1), literal(0.0))(ctx, state).asInstanceOf[NumberValue].doubleValue() should equal(
      Double.PositiveInfinity
    )
    Divide(literal(-1), literal(0.0))(ctx, state).asInstanceOf[NumberValue].doubleValue() should equal(
      Double.NegativeInfinity
    )
    Divide(literal(0), literal(0.0))(ctx, state).asInstanceOf[FloatingPointValue].isNaN shouldBe true
  }
}
