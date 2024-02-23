/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import org.neo4j.cypher.internal.util.RewritableTest.Exp
import org.neo4j.cypher.internal.util.RewritableTest.ExpList
import org.neo4j.cypher.internal.util.RewritableTest.Mappings
import org.neo4j.cypher.internal.util.RewritableTest.Options
import org.neo4j.cypher.internal.util.RewritableTest.Pos
import org.neo4j.cypher.internal.util.RewritableTest.Sum
import org.neo4j.cypher.internal.util.RewritableTest.Val
import org.neo4j.cypher.internal.util.RewritableTest.Wrap
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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

  case class ExpList(args: List[Exp]) extends Exp {

    def dup(children: Seq[AnyRef]): this.type =
      ExpList(children.head.asInstanceOf[List[Exp]]).asInstanceOf[this.type]
  }

  case class Wrap(anyRef: AnyRef) extends Exp {

    def dup(children: Seq[AnyRef]): this.type =
      Wrap(children.head).asInstanceOf[this.type]
  }

  case class Mappings(map: Map[String, Exp]) extends Exp {

    override def dup(children: Seq[AnyRef]): this.type =
      Mappings(children.head.asInstanceOf[Map[String, Exp]]).asInstanceOf[this.type]
  }
}

class RewritableTest extends CypherFunSuite {

  // ---------
  // topDown variants
  // ---------
  List(
    "topDown" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(topDown(Rewriter.lift(rewritePf)))
    ),
    "topDown rightToLeft" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(topDown(Rewriter.lift(rewritePf), leftToRight = false))
    ),
    "topDownWithParent" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) => {
      val liftedPf = Rewriter.lift(rewritePf)
      val f: RewriterWithParent = {
        case (value, _) => liftedPf(value)
      }
      ast.rewrite(topDownWithParent(f))
    })
  ) foreach { case (name, rewrite) =>
    test(s"$name should be identical when no rule matches") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = rewrite(
        ast,
        {
          case None => ???
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when using identity") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = rewrite(
        ast,
        {
          case a => a
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when no rule matches: Rewriting a Map") {
      val ast = Add(Val(1), Mappings(Map("2" -> Val(3), "4" -> Val(5))))

      val result = rewrite(
        ast,
        {
          case None => ???
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when using identity: Rewriting a Map") {
      val ast = Add(Val(1), Mappings(Map("2" -> Val(3))))

      val result = rewrite(
        ast,
        {
          case a => a
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should match and replace primitives") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = rewrite(
        ast,
        {
          case _: java.lang.Integer => 99: java.lang.Integer
        }
      )

      assert(result === Add(Val(99), Add(Val(99), Val(99))))
    }

    test(s"$name should match and replace trees") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = rewrite(
        ast,
        {
          case Add(Val(x), Val(y)) =>
            Val(x + y)
        }
      )

      assert(result === Add(Val(1), Val(5)))
    }

    test(s"$name should match and replace primitives and trees") {
      val ast = Add(Val(8), Add(Val(2), Val(3)))

      val result = rewrite(
        ast,
        {
          case Val(_) =>
            Val(1)
          case Add(Val(x), Val(y)) =>
            Val(x + y)
        }
      )

      assert(result === Add(Val(1), Val(5)))
    }

    test(s"$name should duplicate terms with pair parameters") {
      val ast = Add(Val(1), RewritableTest.Pos((Val(2), Val(3))))

      val result = rewrite(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), Pos((Val(99), Val(99)))))
    }

    test(s"$name should duplicate terms with sequence of pairs") {
      val ast = Add(Val(1), Options(Seq((Val(2), Val(3)), (Val(4), Val(5)))))

      val result = rewrite(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), Options(Seq((Val(99), Val(99)), (Val(99), Val(99))))))
    }

    test(s"$name should duplicate terms with list of pairs") {
      val ast = Add(Val(1), ExpList(List(Val(2), Val(3))))

      val result = rewrite(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), ExpList(List(Val(99), Val(99)))))
    }

    test(s"$name should not change order of Sets of primitives") {
      val ast = Wrap(Set("a", "b", "c"))

      val result = rewrite(
        ast,
        {
          case "a" => "A"
        }
      ).asInstanceOf[Wrap]

      // Turn the Set into a Seq to assert on the order
      assert(result.anyRef.asInstanceOf[Set[String]].toSeq === Seq("A", "b", "c"))
    }

    test(s"$name should not change order of ListSets of primitives") {
      val ast = Wrap(ListSet("a", "b", "c"))

      val result = rewrite(
        ast,
        {
          case "a" => "A"
        }
      ).asInstanceOf[Wrap]

      // Turn the Set into a Seq to assert on the order
      assert(result.anyRef.asInstanceOf[Set[String]].toSeq === Seq("A", "b", "c"))
    }

    test(s"$name should not change order of Maps of primitives") {
      val ast = Wrap(Map("a" -> 10, "b" -> 20))

      val result = rewrite(
        ast,
        {
          case "a" => "A"
        }
      ).asInstanceOf[Wrap]

      // Turn the Map into a Seq to assert on the order
      assert(result.anyRef.asInstanceOf[Map[String, Int]].toSeq === Seq("A" -> 10, "b" -> 20))
    }
  }

  test("topDown should stop with a stopper") {
    val ast = Add(ExpList(List(Val(1))), Val(2))

    val result = ast.rewrite(topDown(
      Rewriter.lift {
        case Val(i) =>
          Val(i * 2)
      },
      stopper = _.isInstanceOf[ExpList]
    ))

    assert(result === Add(ExpList(List(Val(1))), Val(4)))
  }

  // -------------------
  // topDown rightToLeft
  // -------------------
  test("topDown rightToLeft should visit children right to left") {
    val ast = Wrap(Seq("a", "b", "c"))

    val seqBuilder = Seq.newBuilder[String]

    ast.rewrite(topDown(
      Rewriter.lift {
        case x: String =>
          seqBuilder.addOne(x)
          x
      },
      leftToRight = false
    ))

    assert(seqBuilder.result() === Seq("c", "b", "a"))
  }

  // -------------------
  // topDownWithParent
  // -------------------
  test("topDownWithParent should let rules see parent as additional context") {
    val v1 = Val(1)
    val v2 = Val(2)
    val v3 = Val(3)
    val parent = Add(v2, v3)
    val grandParent = Add(v1, parent)

    val parentOf: Map[Exp, Option[Exp]] = Map(
      v1 -> Some(grandParent),
      v2 -> Some(parent),
      v3 -> Some(parent),
      parent -> Some(grandParent),
      grandParent -> None
    )

    val result = grandParent.rewrite(topDownWithParent(RewriterWithParent.lift {
      case (x: Exp, parent) =>
        assert(parent === parentOf(x))
        x
    }))

    assert(result === grandParent)
  }

  // --------------------------------
  // bottomUp, bottomUpWithRecorder
  // --------------------------------
  List(
    "bottomUp" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(bottomUp(Rewriter.lift(rewritePf)))
    ),
    "bottomUpWithRecorder" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(bottomUpWithRecorder(Rewriter.lift(rewritePf)))
    )
  ) foreach { case (name, bottomUpVariant) =>
    test(s"$name should be identical when no rule matches") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(
        ast,
        {
          case None => ???
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when using identity") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(
        ast,
        {
          case a => a
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when no rule matches: Rewriting a Map") {
      val ast = Add(Val(1), Mappings(Map("2" -> Val(3))))

      val result = bottomUpVariant(
        ast,
        {
          case None => ???
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should be identical when using identity: Rewriting a Map") {
      val ast = Add(Val(1), Mappings(Map("2" -> Val(3))))

      val result = bottomUpVariant(
        ast,
        {
          case a => a
        }
      )

      result should be theSameInstanceAs ast
    }

    test(s"$name should match and replace primitives") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(
        ast,
        {
          case _: java.lang.Integer => 99: java.lang.Integer
        }
      )

      assert(result === Add(Val(99), Add(Val(99), Val(99))))
    }

    test(s"$name should match and replace trees") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))

      val result = bottomUpVariant(
        ast,
        {
          case Add(Val(x), Val(y)) =>
            Val(x + y)
        }
      )

      assert(result === Val(6))
    }

    test(s"$name should match and replace primitives and trees") {
      val ast = Add(Val(8), Add(Val(2), Val(3)))

      val result = bottomUpVariant(
        ast,
        {
          case Val(_) =>
            Val(1)
          case Add(Val(x), Val(y)) =>
            Val(x + y)
        }
      )

      assert(result === Val(3))
    }

    test(s"$name should duplicate terms with pair parameters") {
      val ast = Add(Val(1), Pos((Val(2), Val(3))))

      val result = bottomUpVariant(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), Pos((Val(99), Val(99)))))
    }

    test(s"$name should duplicate terms with sequence of pairs") {
      val ast = Add(Val(1), Options(Seq((Val(2), Val(3)), (Val(4), Val(5)))))

      val result = bottomUpVariant(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), Options(Seq((Val(99), Val(99)), (Val(99), Val(99))))))
    }

    test(s"$name should duplicate terms with list of pairs") {
      val ast = Add(Val(1), ExpList(List(Val(2), Val(3))))

      val result = bottomUpVariant(
        ast,
        {
          case Val(_) => Val(99)
        }
      )

      assert(result === Add(Val(99), ExpList(List(Val(99), Val(99)))))
    }
  }

  test("bottomUp should stop with a stopper") {
    val ast = Add(ExpList(List(Val(1))), Val(2))

    val result = ast.rewrite(bottomUp(
      Rewriter.lift {
        case Val(i) =>
          Val(i * 2)
      },
      stopper = _.isInstanceOf[ExpList]
    ))

    assert(result === Add(ExpList(List(Val(1))), Val(4)))
  }

  test("should not create unnecessary copies of objects that have Seq's as Children (when using ListBuffer)") {
    case class Thing(texts: Seq[String]) extends Rewritable {
      def dup(children: Seq[AnyRef]): this.type =
        if (children.iterator eqElements this.treeChildren)
          this
        else {
          Thing(children.head.asInstanceOf[Seq[String]]).asInstanceOf[this.type]
        }
    }
    case object NotUsed

    val thing = Thing(Seq("a", "b", "c"))
    val rewritten = thing.rewrite(bottomUp(Rewriter.lift {
      case NotUsed => NotUsed
    }))

    rewritten should be theSameInstanceAs thing
  }

  class TestCancellationChecker extends CancellationChecker {
    var cancelNext = false
    val message = "my exception"
    override def throwIfCancelled(): Unit = if (cancelNext) throw new RuntimeException(message)
  }

  test(s"topDown should support cancellation") {
    val ast = Sum(Seq(Val(1), Val(1), Val(2), Val(1), Val(1)))

    val cancellation = new TestCancellationChecker

    val rewriter = Rewriter.lift({
      case Val(2) =>
        cancellation.cancelNext = true
        Val(99)
    })

    val e = the[RuntimeException].thrownBy(ast.rewrite(topDown(rewriter, cancellation = cancellation)))

    assert(e.getMessage === cancellation.message)
  }

  test(s"topDownWithParent should support cancellation") {
    val ast = Sum(Seq(Val(1), Val(1), Val(2), Val(1), Val(1)))

    val cancellation = new TestCancellationChecker

    val rewriter = RewriterWithParent.lift({
      case (Val(2), _) =>
        cancellation.cancelNext = true
        Val(99)
    })

    val e = the[RuntimeException].thrownBy(ast.rewrite(topDownWithParent(rewriter, cancellation = cancellation)))

    assert(e.getMessage === cancellation.message)
  }

  test(s"bottomUp should support cancellation") {
    val ast = Sum(Seq(Val(1), Val(1), Val(2), Val(1), Val(1)))

    val cancellation = new TestCancellationChecker

    val rewriter = Rewriter.lift({
      case Val(2) =>
        cancellation.cancelNext = true
        Val(99)
    })

    val e = the[RuntimeException].thrownBy(ast.rewrite(bottomUp(rewriter, cancellation = cancellation)))

    assert(e.getMessage === cancellation.message)
  }

  test(s"bottomUpWithRecorder should support cancellation") {
    val ast = Sum(Seq(Val(1), Val(1), Val(2), Val(1), Val(1)))

    val cancellation = new TestCancellationChecker

    val rewriter = Rewriter.lift({
      case Val(2) =>
        cancellation.cancelNext = true
        Val(99)
    })

    val e = the[RuntimeException].thrownBy(ast.rewrite(bottomUpWithRecorder(rewriter, cancellation = cancellation)))

    assert(e.getMessage === cancellation.message)
  }
}
