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
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.LongValue

class TypeTest extends CypherFunSuite {

  test("plus int int") {
    val op = Add(literal(1), literal(2))

    val result = calc(op)

    // all integer operations should result in longs
    result shouldBe a[LongValue]
  }

  test("plus double int") {
    val op = Add(literal(1.2), literal(2))

    val result = calc(op)

    result shouldBe a[DoubleValue]
  }

  test("minus int int") {
    val op = Subtract(literal(1), literal(2))

    val result = calc(op)

    // all integer operations should result in longs
    result shouldBe a[LongValue]
  }

  test("minus double int") {
    val op = Subtract(literal(1.2), literal(2))

    val result = calc(op)

    result shouldBe a[DoubleValue]
  }

  test("multiply int int") {
    val op = Multiply(literal(1), literal(2))

    val result = calc(op)

    // all integer operations should result in longs
    result shouldBe a[LongValue]
  }

  test("multiply double int") {
    val op = Multiply(literal(1.2), literal(2))

    val result = calc(op)

    result shouldBe a[DoubleValue]
  }

  test("divide int int") {
    val op = Divide(literal(1), literal(2))

    val result = calc(op)

    result shouldBe a[LongValue]
  }

  test("divide double int") {
    val op = Divide(literal(1.2), literal(2))

    val result = calc(op)

    result shouldBe a[DoubleValue]
  }

  private def calc(e: Expression) = e.apply(CypherRow.empty, QueryStateHelper.empty)
}
