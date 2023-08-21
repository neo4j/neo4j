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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Add
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ParameterFromSlot
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.UTF8StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.Values.utf8Value

import java.nio.charset.StandardCharsets

class AddTest extends CypherFunSuite {

  val m = CypherRow.empty
  val s = QueryStateHelper.empty

  test("numbers") {
    val expr = Add(literal(1), literal(1))
    expr(m, s) should equal(longValue(2))
  }

  test("with_null") {
    val nullPlusOne = Add(literal(NO_VALUE), literal(1))
    val twoPlusNull = Add(literal(2), literal(NO_VALUE))

    nullPlusOne(m, s) should equal(NO_VALUE)
    twoPlusNull(m, s) should equal(NO_VALUE)
  }

  test("strings") {
    val expr = Add(literal("hello"), literal("world"))
    expr(m, s) should equal(stringValue("helloworld"))
  }

  test("stringPlusNumber") {
    val expr = Add(literal("hello"), literal(1))
    expr(m, s) should equal(stringValue("hello1"))
  }

  test("numberPlusString") {
    val expr = Add(literal(1), literal("world"))
    expr(m, s) should equal(stringValue("1world"))
  }

  test("numberPlusBool") {
    val expr = Add(literal(1), literal(true))
    intercept[CypherTypeException](expr(m, s))
  }

  test("UTF8 value addition") {
    // Given
    val hello = "hello".getBytes(StandardCharsets.UTF_8)
    val world = "world".getBytes(StandardCharsets.UTF_8)
    val state = QueryStateHelper.emptyWith(params = Array(utf8Value(hello), utf8Value(world)))

    // When
    val result = Add(ParameterFromSlot(0, "p1"), ParameterFromSlot(1, "p2"))(m, state)

    // Then
    result shouldBe a[UTF8StringValue]
    result should equal(utf8Value("helloworld".getBytes(StandardCharsets.UTF_8)))
    result should equal(Values.stringValue("helloworld"))
  }
}
