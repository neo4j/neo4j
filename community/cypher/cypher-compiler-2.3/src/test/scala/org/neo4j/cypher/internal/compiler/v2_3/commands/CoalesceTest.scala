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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{CoalesceFunction, Expression, Literal, Null}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{QueryState, QueryStateHelper}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CoalesceTest extends CypherFunSuite {

  test("givenANonNullValueThenReturnsTheValue") {
    val func = new CoalesceFunction(Literal("a"))
    calc(func) should equal("a")
  }

  test("givenANullValueThenReturnsNull") {
    val func = new CoalesceFunction(Null())
    calc(func) should equal(null.asInstanceOf[Any])
  }

  test("givenOneNullAndOneValueThenReturnsTheValue") {
    val func = new CoalesceFunction(Null(), Literal("Alistair"))
    calc(func) should equal("Alistair")
  }

  test("coalesce_should_be_lazy") {
    val func = new CoalesceFunction(Literal("Hunger"), BreakingExpression())
    calc(func) should equal("Hunger")
  }

  private def calc(e: Expression): Any = e(ExecutionContext.empty)(QueryStateHelper.empty)
}

case class BreakingExpression() extends Expression {
  def apply(v1: ExecutionContext)(implicit state: QueryState) {
    import org.scalatest.Assertions._
    fail("Coalesce is not lazy")
  }

  def rewrite(f: (Expression) => Expression) = null

  def arguments = Nil

  def calculateType(symbols: SymbolTable): CypherType = CTAny

  def symbolTableDependencies = Set()
}
