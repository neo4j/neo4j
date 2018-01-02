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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.ParameterWrongTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ToStringFunctionTest extends CypherFunSuite {

  test("should return null if argument is null") {
    assert(toStringFunction(null) === null)
  }

  test("should not change a string") {
    toStringFunction("10.599") should be("10.599")
  }

  test("should convert an integer to a string") {
    toStringFunction(21) should be("21")
  }

  test("should convert an float to a string") {
    toStringFunction(23.34) should be("23.34")
  }

  test("should convert a negative float to a string") {
    toStringFunction(-12.66) should be("-12.66")
  }

  test("should convert a negative integer to a string") {
    toStringFunction(-12) should be("-12")
  }

  test("should throw an exception if the argument is an object which cannot be converted to a float") {
    evaluating { toStringFunction(new Object) } should produce[ParameterWrongTypeException]
  }

  private def toStringFunction(orig: Any) = {
    ToStringFunction(Literal(orig))(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
