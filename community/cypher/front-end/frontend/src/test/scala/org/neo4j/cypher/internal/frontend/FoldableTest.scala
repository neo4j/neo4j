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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.frontend.FoldableTest.Add
import org.neo4j.cypher.internal.frontend.FoldableTest.Exp
import org.neo4j.cypher.internal.frontend.FoldableTest.Sum
import org.neo4j.cypher.internal.frontend.FoldableTest.Val
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.Foldable.TreeAny
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.jdk.CollectionConverters.ListHasAsScala

object FoldableTest {
  trait Exp extends Foldable
  case class Val(int: Int) extends Exp
  case class Add(lhs: Exp, rhs: Exp) extends Exp
  case class Sum(args: Seq[Exp]) extends Exp
}

class FoldableTest extends CypherFunSuite {

  test("should fold value depth first over object tree") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.folder.fold(50) {
      case Val(x) => acc => acc + x
    }

    assert(result === 200)
  }

  test("should fold by depth then breadth left to right") {
    val ast = Add(Val(1), Add(Add(Val(2), Val(3)), Val(4)))

    val result = ast.folder.fold(Seq.empty[Int]) {
      case Val(x) => acc => acc :+ x
    }

    assert(result === Seq(1, 2, 3, 4))
  }

  test("should tree fold over all objects") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.folder.treeFold(50) {
      case Val(x) => acc => TraverseChildren(acc + x)
    }

    result should be(50 + 55 + 43 + 52)
  }

  test("should be able to stop-recursion in tree fold") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.folder.treeFold(50) {
      case Val(x)          => acc => TraverseChildren(acc + x)
      case Add(Val(43), _) => acc => SkipChildren(acc + 20)
    }

    result should be(50 + 55 + 20)
  }

  test("should be able merge accumulators in tree fold") {
    val ast = Sum(Seq(Val(55), Add(Val(43), Val(52)), Val(10)))

    val result = ast.folder.treeFold(50) {
      case Val(x)    => acc => TraverseChildren(acc + x)
      case Add(_, _) => acc => TraverseChildrenNewAccForSiblings(acc, acc => acc * 2)
    }

    result should be((50 + 55 + 43 + 52) * 2 + 10)
  }

  test("should reverse tree fold over all objects") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.folder.reverseTreeFold("x") {
      case Val(x) => acc => TraverseChildren(acc + "|" + x)
    }

    result should be("x|52|43|55")
  }

  test("should be able to stop-recursion in reverse tree fold") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.folder.reverseTreeFold("x") {
      case Val(x)          => acc => TraverseChildren(acc + "|" + x)
      case Add(Val(43), _) => acc => SkipChildren(acc + "<>")
    }

    result should be("x<>|55")
  }

  test("should be able merge accumulators in reverse tree fold") {
    val ast = Sum(Seq(Val(55), Add(Val(43), Val(52)), Val(10)))

    val result = ast.folder.reverseTreeFold("x") {
      case Val(x)    => acc => TraverseChildren(acc + "|" + x)
      case Add(_, _) => acc => TraverseChildrenNewAccForSiblings(acc + ">", acc => acc + "<")
    }

    result should be("x|10>|52|43<|55")
  }

  test("should allow using exist to find patterns deeply nested") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    ast.folder.treeExists {
      case Val(x) => x == 2
    } should equal(true)

    ast.folder.treeExists {
      case Val(x) => x == 42
    } should equal(false)
  }

  test("should allow using forall to find patterns deeply nested") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    ast.folder.treeForall {
      case Val(x) => x > 0
      case _      => true
    } should equal(true)

    ast.folder.treeForall {
      case Val(x) => x < 3
      case _      => true
    } should equal(false)
  }

  test("should get children") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    ast.treeChildren.toList shouldEqual Seq(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in reverse") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    ast.reverseTreeChildren.toList shouldEqual Seq(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in list") {
    val ast = Seq(Val(1), Add(Val(2), Val(3)))

    ast.treeChildren.toList shouldEqual Seq(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in list in reverse") {
    val ast = Seq(Val(1), Add(Val(2), Val(3)))

    ast.reverseTreeChildren.toList shouldEqual Seq(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in wrapping list") {
    val ast = java.util.List.of(Val(1), Add(Val(2), Val(3))).asScala

    ast.treeChildren.toList shouldEqual Seq(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in wrapping list in reverse") {
    val ast = java.util.List.of(Val(1), Add(Val(2), Val(3))).asScala

    ast.reverseTreeChildren.toList shouldEqual Seq(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in Vector") {
    val ast = Vector(Val(1), Add(Val(2), Val(3)))

    ast.treeChildren.toList shouldEqual Seq(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in Vector in reverse") {
    val ast = Vector(Val(1), Add(Val(2), Val(3)))

    ast.reverseTreeChildren.toList shouldEqual Seq(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in colon list") {
    val list = List(Val(1), Add(Val(2), Val(3)))

    list.treeChildren.toList shouldEqual List(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in colon in reverse") {
    val list = List(Val(1), Add(Val(2), Val(3)))

    list.reverseTreeChildren.toList shouldEqual List(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in ListSet") {
    val list = ListSet(Val(1), Add(Val(2), Val(3)))

    list.treeChildren.toList shouldEqual List(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in ListSet in reverse") {
    val list = ListSet(Val(1), Add(Val(2), Val(3)))

    list.reverseTreeChildren.toList shouldEqual List(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in scala ListSet in reverse") {
    val list = scala.collection.immutable.ListSet(Val(1), Add(Val(2), Val(3)))

    list.reverseTreeChildren.toList shouldEqual List(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in Set") {
    val list = Set(Val(1), Add(Val(2), Val(3)))

    list.treeChildren.toList shouldEqual List(Val(1), Add(Val(2), Val(3)))
  }

  test("should get children in Set in reverse") {
    val list = Set(Val(1), Add(Val(2), Val(3)))

    list.reverseTreeChildren.toList shouldEqual List(Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in Map") {
    val list = Map(Val(1) -> Add(Val(2), Val(3)), Val(2) -> Val(10))

    list.treeChildren.toList shouldEqual List(Val(1), Add(Val(2), Val(3)), Val(2), Val(10))
  }

  test("should get children in Map in reverse") {
    val list = Map(Val(1) -> Add(Val(2), Val(3)), Val(2) -> Val(10))

    list.reverseTreeChildren.toList shouldEqual List(Val(10), Val(2), Add(Val(2), Val(3)), Val(1))
  }

  test("should get children in Option") {
    val ast = Some(Val(1))

    ast.treeChildren.toList shouldEqual Seq(Val(1))
  }

  test("should get children in Option in reverse") {
    val ast = Some(Val(1))

    ast.reverseTreeChildren.toList shouldEqual Seq(Val(1))
  }

  test("should not get any children in empty lists") {
    val astEmptyList = Seq.empty[FoldableTest]
    val astEmptyOption = Option.empty[FoldableTest]
    val astEmptyWrappingList = java.util.List.of[FoldableTest]()
    val astEmptyVector = Vector.empty[FoldableTest]

    astEmptyList.treeChildren.toList shouldEqual Seq.empty
    astEmptyList.reverseTreeChildren.toList shouldEqual Seq.empty
    astEmptyOption.treeChildren.toList shouldEqual Seq.empty
    astEmptyOption.reverseTreeChildren.toList shouldEqual Seq.empty
    astEmptyWrappingList.treeChildren.toList shouldEqual Seq.empty
    astEmptyWrappingList.reverseTreeChildren.toList shouldEqual Seq.empty
    astEmptyVector.treeChildren.toList shouldEqual Seq.empty
    astEmptyVector.reverseTreeChildren.toList shouldEqual Seq.empty
  }

  class TestCancellationChecker extends CancellationChecker {
    var cancelNext = false
    val message = "my exception"

    override def throwIfCancelled(): Unit = if (cancelNext) throw new RuntimeException(message)
  }

  class TestCountdownCancellationChecker(var count: Int) extends CancellationChecker {
    val message = "my exception"

    override def throwIfCancelled(): Unit = {
      count -= 1
      if (count <= 0) throw new RuntimeException(message)
    }
  }

  test("fold should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCancellationChecker
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).fold(0) {
        case Val(3) =>
          cancellation.cancelNext = true
          acc => acc + 1
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeFold should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCancellationChecker
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeFold(0) {
        case Val(3) =>
          cancellation.cancelNext = true
          acc => TraverseChildren(acc + 1)
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("reverseTreeFold should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCancellationChecker
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).reverseTreeFold(0) {
        case Val(3) =>
          cancellation.cancelNext = true
          acc => TraverseChildren(acc + 1)
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeExists should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCancellationChecker
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeExists {
        case Val(3) =>
          cancellation.cancelNext = true
          false
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeForall should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCancellationChecker
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeForall {
        case Val(3) =>
          cancellation.cancelNext = true
          true
        case _ => true
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeForall should stop early") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    ast.folder.treeForall {
      case Val(1) => true
      case Val(2) => false
      case x: Val => throw new IllegalStateException(s"Not expected to come here: $x")
      case _      => true
    } shouldEqual false
  }

  test("treeFind should only find if the partial function returns true") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    ast.folder.treeFind[Exp] {
      case Val(3) => true
      case Val(1) => false
    } should equal(Some(Val(3)))
  }

  test("treeFind should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCountdownCancellationChecker(2)
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeFind[Exp] {
        case Val(3) => true
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeFindByClass should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCountdownCancellationChecker(2)
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeFindByClass[Val]
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeCount should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCountdownCancellationChecker(2)
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeCount {
        case _: Val => ()
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("treeCountAccumulation should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCountdownCancellationChecker(2)
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).treeCountAccumulation {
        case _: Val => 1
      }
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }

  test("findAllByClass should support cancelling") {
    val ast = Sum(Seq(Val(1), Val(2), Val(3), Val(4), Val(5)))

    val cancellation = new TestCountdownCancellationChecker(2)
    val ex = the[Exception].thrownBy(
      ast.folder(cancellation).findAllByClass[Val]
    )

    ex.getMessage.shouldEqual(cancellation.message)
  }
}
