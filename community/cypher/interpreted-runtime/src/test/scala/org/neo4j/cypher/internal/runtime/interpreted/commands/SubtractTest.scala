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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Subtract
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.storable.Values.longValue

class SubtractTest extends CypherFunSuite {

  val m = CypherRow.empty
  val s = QueryStateHelper.empty

  test("numbers") {
    val expr = Subtract(literal(2), literal(1))
    expr(m, s) should equal(longValue(1))
  }

  test("strings") {
    val expr = Subtract(literal("hello"), literal("world"))
    intercept[CypherTypeException](expr(m, s))
  }

  test("stringPlusNumber") {
    val expr = Subtract(literal("hello"), literal(1))
    intercept[CypherTypeException](expr(m, s))
  }

  test("numberPlusString") {
    val expr = Subtract(literal(1), literal("world"))
    intercept[CypherTypeException](expr(m, s))
  }

  test("numberPlusBool") {
    val expr = Subtract(literal("1"), literal(true))
    intercept[CypherTypeException](expr(m, s))
  }
}
