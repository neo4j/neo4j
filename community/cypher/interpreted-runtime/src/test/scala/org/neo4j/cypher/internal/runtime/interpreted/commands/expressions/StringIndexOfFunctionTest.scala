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
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

class StringIndexOfFunctionTest extends InterpretedRuntimeTestSuite {

  test("passing null to string.indexOf() returns null") {
    indexOf(null, "a") should be(NO_VALUE)
    indexOf("a", null) should be(NO_VALUE)
    indexOf(null, null) should be(NO_VALUE)
  }

  test("string.indexOf() should return correct index") {
    indexOf("hello", "e") should be(intValue(1))
    indexOf("hello", "l") should be(intValue(2))
    indexOf("hello", "lo") should be(intValue(3))
    indexOf("hello", "hello") should be(intValue(0))
  }

  test("string.indexOf() should return -1 if not found") {
    indexOf("hello", "x") should be(intValue(-1))
    indexOf("hello", "H") should be(intValue(-1))
  }

  test("string.indexOf() with empty strings") {
    indexOf("hello", "") should be(intValue(0))
    indexOf("", "a") should be(intValue(-1))
    indexOf("", "") should be(intValue(0))
  }

  test("string.indexOf() with unicode") {
    indexOf("åäö", "ä") should be(intValue(1))
    indexOf("😊😂", "😂") should be(intValue(1))
  }

  test("string.indexOf() should throw CypherTypeException for non-string input") {
    intercept[CypherTypeException] {
      indexOf(1, "a")
    }.getMessage should include("Expected a string value for `string.indexOf`, but got: Int(1)")

    intercept[CypherTypeException] {
      indexOf("a", 1)
    }.getMessage should include("Expected a string value for `string.indexOf`, but got: Int(1)")
  }

  private def indexOf(input: Any, value: Any) = {
    val expr = StringIndexOfFunction(literal(input), literal(value))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }
}
