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
package org.neo4j.cypher.internal.frontend.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

object RewritableTest {
  trait Exp extends Product with Rewritable
  case class Val(int: Int) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Val(children(0).asInstanceOf[Int]).asInstanceOf[this.type]
  }
  case class Add(lhs: Exp, rhs: Exp) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Add(children(0).asInstanceOf[Exp], children(1).asInstanceOf[Exp]).asInstanceOf[this.type]
  }
  case class Sum(args: Seq[Exp]) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Sum(children(0).asInstanceOf[Seq[Exp]]).asInstanceOf[this.type]
  }
  case class Pos(latlng: (Exp, Exp)) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Pos(children(0).asInstanceOf[(Exp, Exp)]).asInstanceOf[this.type]
  }
  case class Options(args: Seq[(Exp, Exp)]) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Options(children(0).asInstanceOf[Seq[(Exp, Exp)]]).asInstanceOf[this.type]
  }
}

class RewritableTest extends CypherFunSuite {
  import RewritableTest._

  test("topDown should be identical when no rule matches") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case None => ???
    }))

    assert(result === ast)
  }

  test("topDown should be identical when using identity") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case a => a
    }))

    assert(result === ast)
  }

  test("topDown should match and replace primitives") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case _: java.lang.Integer => 99: java.lang.Integer
    }))

    assert(result === Add(Val(99), Add(Val(99), Val(99))))
  }

  test("topDown should match and replace trees") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Add(Val(1), Val(5)))
  }

  test("topDown should match and replace primitives and trees") {
    val ast = Add(Val(8), Add(Val(2), Val(3)))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case Val(_) =>
        Val(1)
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Add(Val(1), Val(5)))
  }

  test("topDown should duplicate terms with pair parameters") {
    val ast = Add(Val(1), Pos((Val(2), Val(3))))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case Val(_) => Val(99)
    }))

    assert(result === Add(Val(99), Pos((Val(99), Val(99)))))
  }

  test("topDown should duplicate terms with sequence of pairs") {
    val ast = Add(Val(1), Options(Seq((Val(2), Val(3)), (Val(4), Val(5)))))

    val result = ast.rewrite(topDown(Rewriter.lift {
      case Val(_) => Val(99)
    }))

    assert(result === Add(Val(99), Options(Seq((Val(99), Val(99)), (Val(99), Val(99))))))
  }

  test("bottomUp should be identical when no rule matches") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case None => ???
    }))

    assert(result === ast)
  }

  test("bottomUp should be identical when using identity") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case a => a
    }))

    assert(result === ast)
  }

  test("bottomUp should match and replace primitives") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case _: java.lang.Integer => 99: java.lang.Integer
    }))

    assert(result === Add(Val(99), Add(Val(99), Val(99))))
  }

  test("bottomUp should match and replace trees") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Val(6))
  }

  test("bottomUp should match and replace primitives and trees") {
    val ast = Add(Val(8), Add(Val(2), Val(3)))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Val(_) =>
        Val(1)
      case Add(Val(x), Val(y)) =>
        Val(x + y)
    }))

    assert(result === Val(3))
  }

  test("bottomUp should duplicate terms with pair parameters") {
    val ast = Add(Val(1), Pos((Val(2), Val(3))))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Val(_) => Val(99)
    }))

    assert(result === Add(Val(99), Pos((Val(99), Val(99)))))
  }

  test("bottomUp should duplicate terms with sequence of pairs") {
    val ast = Add(Val(1), Options(Seq((Val(2), Val(3)), (Val(4), Val(5)))))

    val result = ast.rewrite(bottomUp(Rewriter.lift {
      case Val(_) => Val(99)
    }))

    assert(result === Add(Val(99), Options(Seq((Val(99), Val(99)), (Val(99), Val(99))))))
  }
}
