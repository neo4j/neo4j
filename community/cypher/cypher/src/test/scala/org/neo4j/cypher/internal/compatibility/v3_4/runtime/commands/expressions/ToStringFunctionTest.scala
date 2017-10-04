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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.ParameterWrongTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}

class ToStringFunctionTest extends CypherFunSuite {

  test("should return null if argument is null") {
    assert(toStringFunction(null) === NO_VALUE)
  }

  test("should not change a string") {
    toStringFunction("10.599") should be(stringValue("10.599"))
  }

  test("should convert an integer to a string") {
    toStringFunction(21) should be(stringValue("21"))
  }

  test("should convert a float to a string") {
    toStringFunction(23.34) should be(stringValue("23.34"))
  }

  test("should convert a negative float to a string") {
    toStringFunction(-12.66) should be(stringValue("-12.66"))
  }

  test("should convert a negative integer to a string") {
    toStringFunction(-12) should be(stringValue("-12"))
  }

  test("should handle boolean false") {
    toStringFunction(false) should be(stringValue("false"))
  }

  test("should handle boolean true") {
    toStringFunction(true) should be(stringValue("true"))
  }

  test("should throw an exception if the argument is an object which cannot be converted to a string") {
    val caughtException = evaluating { toStringFunction(List(1,24))} should produce[ParameterWrongTypeException]
    caughtException.getMessage should startWith("Expected a String, Number or Boolean, got: ")
  }

  private def toStringFunction(orig: Any) = {
    ToStringFunction(Literal(orig))(ExecutionContext.empty, QueryStateHelper.empty)
  }
}
