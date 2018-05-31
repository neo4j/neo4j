/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.conditions

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{ASTNode, DummyPosition}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.ast.conditions.orderByOnlyOnVariables
import org.opencypher.v9_0.expressions.{Expression, UnsignedDecimalIntegerLiteral}

class OrderByOnlyOnVariablesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = orderByOnlyOnVariables

  test("unhappy when when order by sort on non-variable expressions") {
    val expr: Expression = UnsignedDecimalIntegerLiteral("42")_
    val orderByPos = DummyPosition(42)
    val ast: ASTNode = Return(false, ReturnItems(false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, Some(OrderBy(Seq(AscSortItem(expr)_))(orderByPos)), None, None)_

    condition(ast) should equal(Seq(s"OrderBy at $orderByPos is ordering on an expression ($expr) instead of a variable"))
  }

  test("happy when order by sort on variable") {
    val ast: ASTNode = Return(false, ReturnItems(false, Seq(AliasedReturnItem(varFor("n"), varFor("n"))_))_, None, Some(OrderBy(Seq(AscSortItem(varFor("n"))_))(pos)), None, None)_

    condition(ast) shouldBe empty
  }
}
