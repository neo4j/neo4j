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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{CoalesceFunction, Expression, Literal, Null}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue

class CoalesceTest extends CypherFunSuite {

  test("givenANonNullValueThenReturnsTheValue") {
    val func = CoalesceFunction(Literal("a"))
    calc(func) should equal(stringValue("a"))
  }

  test("givenANullValueThenReturnsNull") {
    val func = CoalesceFunction(Null())
    calc(func) should equal(Values.NO_VALUE)
  }

  test("givenOneNullAndOneValueThenReturnsTheValue") {
    val func = CoalesceFunction(Null(), Literal("Alistair"))
    calc(func) should equal(stringValue("Alistair"))
  }

  test("coalesce_should_be_lazy") {
    val func = CoalesceFunction(Literal("Hunger"), BreakingExpression())
    calc(func) should equal(stringValue("Hunger"))
  }

  private def calc(e: Expression): Any = e(ExecutionContext.empty, QueryStateHelper.empty)
}

case class BreakingExpression() extends Expression {
  def apply(v1: ExecutionContext, state: QueryState) = {
    import org.scalatest.Assertions._
    fail("Coalesce is not lazy")
  }

  def rewrite(f: (Expression) => Expression) = null

  def arguments = Nil

  def symbolTableDependencies = Set()
}
