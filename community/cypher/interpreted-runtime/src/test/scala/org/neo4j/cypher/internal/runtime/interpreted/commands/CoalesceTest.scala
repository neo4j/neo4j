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
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CoalesceFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Null
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.scalatest.Assertions.fail

class CoalesceTest extends CypherFunSuite {

  test("givenANonNullValueThenReturnsTheValue") {
    val func = CoalesceFunction(literal("a"))
    calc(func) should equal(stringValue("a"))
  }

  test("givenANullValueThenReturnsNull") {
    val func = CoalesceFunction(Null())
    calc(func) should equal(Values.NO_VALUE)
  }

  test("givenOneNullAndOneValueThenReturnsTheValue") {
    val func = CoalesceFunction(Null(), literal("Alistair"))
    calc(func) should equal(stringValue("Alistair"))
  }

  test("coalesce_should_be_lazy") {
    val func = CoalesceFunction(literal("Hunger"), BreakingExpression())
    calc(func) should equal(stringValue("Hunger"))
  }

  private def calc(e: Expression): Any = e(CypherRow.empty, QueryStateHelper.empty)
}

case class BreakingExpression() extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    fail("Coalesce is not lazy")
  }

  override def rewrite(f: Expression => Expression): Expression = null

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty
}
