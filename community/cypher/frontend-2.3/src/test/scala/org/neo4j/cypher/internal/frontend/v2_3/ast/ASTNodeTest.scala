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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ASTNodeTest extends CypherFunSuite {

  trait Exp extends ASTNode with ASTExpression {
    val position = DummyPosition(0)
  }

  case class Val(int: Int) extends Exp
  case class Add(lhs: Exp, rhs: Exp) extends Exp

  test("rewrite should match and replace expressions") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Val(6))
  }

  test("rewrite should match and replace primitives and expressions") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Val(i) =>
        Val(i * i)
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Val(14))
  }

  test("rewrite should duplicate ASTNode carrying InputPosition") {
    case class AddWithPos(lhs: Exp, rhs: Exp)(override val position: InputPosition) extends Exp

    val ast = Add(Val(1), AddWithPos(Val(2), Val(3))(DummyPosition(0)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case Val(_) => Val(99)
    }))

    assert(result === Add(Val(99), AddWithPos(Val(99), Val(99))(DummyPosition(0))))
  }
}
