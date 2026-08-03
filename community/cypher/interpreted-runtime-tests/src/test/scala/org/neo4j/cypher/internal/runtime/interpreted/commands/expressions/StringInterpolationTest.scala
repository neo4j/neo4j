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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.StringInterpolation
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue

class StringInterpolationTest extends InterpretedRuntimeTestSuite {

  private val m = CypherRow.empty
  private val s = QueryStateHelper.empty

  test("plain literal, no expressions") {
    StringInterpolation(Seq(literal("hello world")), Seq.empty)(m, s) should equal(stringValue("hello world"))
  }

  test("single expression embeds unquoted string") {
    StringInterpolation(Seq(literal("Hello, "), literal("!")), Seq(literal("Pelle")))(m, s) should equal(
      stringValue("Hello, Pelle!")
    )
  }

  test("integer, float and boolean expressions render plainly") {
    StringInterpolation(Seq(literal("v="), literal("")), Seq(literal(1)))(m, s) should equal(stringValue("v=1"))
    StringInterpolation(Seq(literal("v="), literal("")), Seq(literal(3.14)))(m, s) should equal(stringValue("v=3.14"))
    StringInterpolation(Seq(literal("v="), literal("")), Seq(literal(true)))(m, s) should equal(stringValue("v=true"))
  }

  test("any null expression makes the whole result null") {
    StringInterpolation(Seq(literal("a="), literal(", b="), literal("")), Seq(literal(1), literal(NO_VALUE)))(
      m,
      s
    ) should equal(NO_VALUE)
    StringInterpolation(Seq(literal("v="), literal("")), Seq(literal(NO_VALUE)))(m, s) should equal(NO_VALUE)
  }

  test("adjacent expressions with empty literal between them") {
    StringInterpolation(Seq(literal(""), literal(""), literal("")), Seq(literal("foo"), literal("bar")))(
      m,
      s
    ) should equal(stringValue("foobar"))
  }
}
