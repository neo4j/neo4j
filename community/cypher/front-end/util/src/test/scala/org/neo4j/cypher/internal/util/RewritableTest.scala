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
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.RewritableTest.Add
import org.neo4j.cypher.internal.util.RewritableTest.ArrayHolder
import org.neo4j.cypher.internal.util.RewritableTest.Exp
import org.neo4j.cypher.internal.util.RewritableTest.ExpList
import org.neo4j.cypher.internal.util.RewritableTest.Mappings
import org.neo4j.cypher.internal.util.RewritableTest.NodeWithId
import org.neo4j.cypher.internal.util.RewritableTest.Options
import org.neo4j.cypher.internal.util.RewritableTest.Pos
import org.neo4j.cypher.internal.util.RewritableTest.Sum
import org.neo4j.cypher.internal.util.RewritableTest.Val
import org.neo4j.cypher.internal.util.RewritableTest.Wrap
import org.neo4j.cypher.internal.util.attribution.Id
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

  // Deliberately a plain Product without a custom dup, so reconstruction goes through the
  // reflective copy. The bare value class field is the interesting part: its erased copy
  // signature takes an int, which reflection cannot unbox an Id box into.
  case class NodeWithId(id: Id, name: String)

  // Mirrors the execution graph's `middleBuffers: Array[BufferDefinition]`: a typed array of a
  // reference type, which is the shape the Array support in dupAny exists for.
  case class ArrayHolder(args: Array[Exp]) extends Exp {

    def dup(children: Seq[AnyRef]): this.type =
      ArrayHolder(children.head.asInstanceOf[Array[Exp]]).asInstanceOf[this.type]
  }
}

class RewritableTest extends CypherFunSuite {

  // ---------
  // topDown variants
  // ---------
  List(
    "topDown" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(topDown.onRewriter(Rewriter.lift(rewritePf)))
    ),
    "topDown rightToLeft" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(topDown.onRewriter(Rewriter.lift(rewritePf), leftToRight = false))
    ),
    "topDownWithParent" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) => {
      val liftedPf = Rewriter.lift(rewritePf)
      val f: RewriterWithParent = {
        case (value, _) => liftedPf(value)
      }
      ast.rewrite(topDownWithParent(f, cancellation = CancellationChecker.neverCancelled()))
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

    test(s"$name should be identical when no rule matches, big integer number") {
      val ast = Add(Val(1), Add(Val(854), Val(3)))

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
          case x => x
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
          case x => x
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

    val result = ast.rewrite(topDown.onRewriter(
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

    ast.rewrite(topDown.onRewriter(
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

    val result = grandParent.rewrite(topDownWithParent(
      RewriterWithParent.lift {
        case (x: Exp, p) =>
          assert(p === parentOf(x))
          x
      },
      cancellation = CancellationChecker.neverCancelled()
    ))

    assert(result === grandParent)
  }

  // --------------------------------
  // bottomUp, bottomUpWithRecorder
  // --------------------------------
  List(
    "bottomUp" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(bottomUp.onRewriter(Rewriter.lift(rewritePf)))
    ),
    "bottomUpWithRecorder" -> ((ast: Rewritable, rewritePf: PartialFunction[AnyRef, AnyRef]) =>
      ast.rewrite(bottomUpWithRecorder.onRewriter(
        Rewriter.lift(rewritePf),
        cancellation = CancellationChecker.neverCancelled()
      ))
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
          case x => x
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
          case x => x
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

    test(s"$name should hit elements in DFS order") {
      val ast = Add(Val(1), Add(Val(2), Val(3)))
      var previous = 0
      val result = bottomUpVariant(
        ast,
        {
          case exp: Exp =>
            previous = previous + 1
            Mappings(Map(previous.toString -> exp))
        }
      )

      assert(result ===
        Mappings(Map("5" -> Add(
          Mappings(Map("1" -> Val(1))),
          Mappings(Map("4" -> Add(
            Mappings(Map("2" -> Val(2))),
            Mappings(Map("3" -> Val(3)))
          )))
        ))))
    }
  }

  test("bottomUp should stop with a stopper") {
    val ast = Add(ExpList(List(Val(1))), Val(2))

    val result = ast.rewrite(bottomUp.onRewriter(
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
    val rewritten = thing.rewrite(bottomUp.onRewriter(Rewriter.lift {
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

    val e = the[RuntimeException].thrownBy(ast.rewrite(topDown.onRewriter(rewriter, cancellation = cancellation)))

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

    val e = the[RuntimeException].thrownBy(ast.rewrite(bottomUp.onRewriter(rewriter, cancellation = cancellation)))

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

    val e = the[RuntimeException].thrownBy(ast.rewrite(bottomUpWithRecorder.onRewriter(
      rewriter,
      cancellation = cancellation
    )))

    assert(e.getMessage === cancellation.message)
  }

  test("eqElements should compare value class children by value") {
    // Value class children are boxed anew on every productIterator access, so they are never
    // reference-identical between two traversals of the same node.
    val node = NodeWithId(Id(7), "name")
    val first = node.treeChildren.toSeq
    (first.iterator eqElements node.treeChildren) shouldBe true
    (Seq(Id(8).asInstanceOf[AnyRef], "name").iterator eqElements node.treeChildren) shouldBe false
  }

  test("eqElements should compare null children without dereferencing them") {
    // A partially parsed AST can hold null children.
    // (This can happen on the semantic-analysis-js path, which keeps building an AST after a rule fails
    //  and then rewrites it to filter the nulls out).
    val node = NodeWithId(Id(7), null)
    (node.treeChildren.toSeq.iterator eqElements node.treeChildren) shouldBe true
    (node.treeChildren.toSeq.iterator eqElements Seq(Id(7).asInstanceOf[AnyRef], "name").iterator) shouldBe false
    (Seq(Id(7).asInstanceOf[AnyRef], "name").iterator eqElements node.treeChildren) shouldBe false
  }

  test("bottomUp should not reconstruct an unchanged node with a bare Id child") {
    val node = NodeWithId(Id(7), "name")
    val result = node.rewrite(bottomUp(Rewriter.lift { case Val(99) => Val(99) }))
    result should be theSameInstanceAs node
  }

  test("bottomUp should not reconstruct a node whose Array child is unchanged") {
    val holder = ArrayHolder(Array(Val(1), Val(2)))
    val result = holder.rewrite(bottomUp(Rewriter.lift { case Val(99) => Val(99) }))
    result should be theSameInstanceAs holder
  }

  test("bottomUp should rebuild an Array child when an element is rewritten") {
    val holder = ArrayHolder(Array(Val(1), Val(2)))
    val result = holder.rewrite(bottomUp(Rewriter.lift { case Val(1) => Val(99) }))
    result should not be theSameInstanceAs(holder)
    result.asInstanceOf[ArrayHolder].args.toSeq should equal(Seq(Val(99), Val(2)))
  }

  test("dupAny should preserve the component type of a rebuilt Array") {
    val original = Array[Exp](Val(1), Val(2))
    val rebuilt = Rewritable.dupAny(original, Seq(Val(99), Val(2)))
    rebuilt.getClass.getComponentType should equal(classOf[Exp])
  }

  test("dupAny should rebuild a primitive Array as a primitive Array") {
    val original = Array(1, 2)
    val rebuilt = Rewritable.dupAny(original, Seq(Int.box(99), Int.box(2)))
    rebuilt.getClass.getComponentType should equal(java.lang.Integer.TYPE)
    rebuilt.asInstanceOf[Array[Int]].toSeq should equal(Seq(99, 2))
  }
}
