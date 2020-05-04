/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.Foldable.TreeAny
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.internal.util.RewritableTest.Add
import org.neo4j.cypher.internal.util.RewritableTest.Options
import org.neo4j.cypher.internal.util.RewritableTest.Pos
import org.neo4j.cypher.internal.util.RewritableTest.Val
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ListBuffer

object RewritableTest {
  trait Exp extends Product with Rewritable
  case class Val(int: Int) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Val(children.head.asInstanceOf[Int]).asInstanceOf[this.type]
  }
  case class Add(lhs: Exp, rhs: Exp) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Add(children.head.asInstanceOf[Exp], children(1).asInstanceOf[Exp]).asInstanceOf[this.type]
  }
  case class Sum(args: Seq[Exp]) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Sum(children.head.asInstanceOf[Seq[Exp]]).asInstanceOf[this.type]
  }
  case class Pos(latlng: (Exp, Exp)) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Pos(children.head.asInstanceOf[(Exp, Exp)]).asInstanceOf[this.type]
  }
  case class Options(args: Seq[(Exp, Exp)]) extends Exp {
    def dup(children: Seq[AnyRef]): this.type =
      Options(children.head.asInstanceOf[Seq[(Exp, Exp)]]).asInstanceOf[this.type]
  }
}

class RewritableTest extends CypherFunSuite {

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
    val ast = Add(Val(1), RewritableTest.Pos((Val(2), Val(3))))

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
  
  List("bottomUp" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) => ast.rewrite(bottomUp(Rewriter.lift(rewritePf)))),
       "bottomUpWithRecorder" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) => ast.rewrite(bottomUpWithRecorder(Rewriter.lift(rewritePf))))
       ) foreach { case (name, bottomUpVariant) =>
    test(s"$name should be identical when no rule matches") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(ast, {
        case None => ???
      })

      assert(result === ast)
    }

    test(s"$name should be identical when using identity") {
      val ast= Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(ast, {
        case a => a
      })

      assert(result === ast)
    }

    test(s"$name should match and replace primitives") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(ast, {
        case _: java.lang.Integer => 99: java.lang.Integer
      })

      assert(result === Add(Val(99), Add(Val(99), Val(99))))
    }

    test(s"$name should match and replace trees") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(ast, {
        case Add(Val(x), Val(y)) =>
          Val(x + y)
      })

      assert(result === Val(6))
    }

    test(s"$name should match and replace primitives and trees") {
      val ast = Add(Val(8), Add(Val(2), Val(3)))

      val result = bottomUpVariant(ast, {
        case Val(_) =>
          Val(1)
        case Add(Val(x), Val(y)) =>
          Val(x + y)
      })

      assert(result === Val(3))
    }

    test(s"$name should duplicate terms with pair parameters") {
      val ast = Add(Val(1), Pos((Val(2), Val(3))))

      val result = bottomUpVariant(ast, {
        case Val(_) => Val(99)
      })

      assert(result === Add(Val(99), Pos((Val(99), Val(99)))))
    }

    test(s"$name should duplicate terms with sequence of pairs") {
      val ast = Add(Val(1), Options(Seq((Val(2), Val(3)), (Val(4), Val(5)))))

      val result = bottomUpVariant(ast, {
        case Val(_) => Val(99)
      })

      assert(result === Add(Val(99), Options(Seq((Val(99), Val(99)), (Val(99), Val(99))))))
    }
  }

  test("should not create unnecessary copies of objects that have Seq's as Children (when using ListBuffer)") {
    case class Thing(texts: Seq[String]) extends Rewritable {
      def dup(children: Seq[AnyRef]): this.type =
        if (children.iterator eqElements this.children)
          this
        else {
          Thing(children.head.asInstanceOf[Seq[String]]).asInstanceOf[this.type]
        }
    }
    case object NotUsed

    val thing = Thing(ListBuffer("a", "b", "c"))
    val rewritten = thing.rewrite(bottomUp(Rewriter.lift {
      case NotUsed => NotUsed
    }))

    rewritten should be theSameInstanceAs thing
  }
}
