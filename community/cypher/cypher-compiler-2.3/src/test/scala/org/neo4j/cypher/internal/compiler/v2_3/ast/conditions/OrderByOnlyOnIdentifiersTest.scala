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
package org.neo4j.cypher.internal.compiler.v2_3.ast.conditions

import org.neo4j.cypher.internal.frontend.v2_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class OrderByOnlyOnIdentifiersTest extends CypherFunSuite with AstConstructionTestSupport {

  private val condition: (Any => Seq[String]) = orderByOnlyOnIdentifiers

  test("unhappy when when order by sort on non-identifier expressions") {
    val expr: Expression = UnsignedDecimalIntegerLiteral("42")_
    val orderByPos = DummyPosition(42)
    val ast: ASTNode = Return(false, ReturnItems(false, Seq(AliasedReturnItem(ident("n"), ident("n"))_))_, Some(OrderBy(Seq(AscSortItem(expr)_))(orderByPos)), None, None)_

    condition(ast) should equal(Seq(s"OrderBy at $orderByPos is ordering on an expression ($expr) instead of an identifier"))
  }

  test("happy when order by sort on identifier") {
    val ast: ASTNode = Return(false, ReturnItems(false, Seq(AliasedReturnItem(ident("n"), ident("n"))_))_, Some(OrderBy(Seq(AscSortItem(ident("n"))_))_), None, None)_

    condition(ast) shouldBe empty
  }
}
