/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

class ASTNodeTest extends CypherFunSuite {

  trait Exp extends ASTNode {
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
