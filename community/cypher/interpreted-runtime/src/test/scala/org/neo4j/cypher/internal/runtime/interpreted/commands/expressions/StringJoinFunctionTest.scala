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
import org.neo4j.values.storable.Values.stringValue

class StringJoinFunctionTest extends InterpretedRuntimeTestSuite {

  test("passing null to string.join() returns null") {
    join(null, ",") should be(NO_VALUE)
    join(List("a", "b"), null) should be(NO_VALUE)
    join(null, null) should be(NO_VALUE)
  }

  test("string.join() should join strings with delimiter") {
    join(List("a", "b", "c"), ",") should be(stringValue("a,b,c"))
    join(List("a", "b", "c"), "--") should be(stringValue("a--b--c"))
  }

  test("string.join() with empty list") {
    join(List.empty[String], ",") should be(stringValue(""))
  }

  test("string.join() with single item") {
    join(List("a"), ",") should be(stringValue("a"))
  }

  test("string.join() with empty delimiter") {
    join(List("a", "b", "c"), "") should be(stringValue("abc"))
  }

  test("string.join() should ignore nulls in list") {
    join(List("a", null, "c"), ",") should be(stringValue("a,c"))
    join(List(null, "b", null), ",") should be(stringValue("b"))
    join(List(null, null), ",") should be(stringValue(""))
  }

  test("string.join() should throw CypherTypeException for non-list input") {
    intercept[CypherTypeException] {
      join("not a list", ",")
    }.getMessage should include("Expected String(\"not a list\") to be a list")
  }

  test("string.join() should throw CypherTypeException for non-string delimiter") {
    intercept[CypherTypeException] {
      join(List("a", "b"), 1)
    }.getMessage should include("Expected a string value for `string.join`, but got: Int(1)")
  }

  test("string.join() should throw CypherTypeException for non-string items in list") {
    intercept[CypherTypeException] {
      join(List[Any]("a", 1), ",")
    }.getMessage should include("Expected a string value for `string.join`, but got: Int(1)")
  }

  private def join(list: Any, delimiter: Any) = {
    val expr = StringJoinFunction(literal(list), literal(delimiter))
    expr(CypherRow.empty, QueryStateHelper.empty)
  }
}
