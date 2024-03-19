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
package org.neo4j.cypher.internal.util.collection.immutable

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen

import scala.collection.immutable

/**
 * Testing only the pieces which differ from the scala implementation.
 */
class ListSetTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  test("Can build a ListSet from a distinct Seq, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 4, 5)
    ListSet.from(seq).toSeq should equal(seq)
  }

  test("Can build a ListSet from a non-distinct Seq, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 1, 4, 4, 5)
    ListSet.from(seq).toSeq should equal(seq.distinct)
  }

  test("Can build a ListSet from a scala ListSet, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 4, 5)
    val ls = scala.collection.immutable.ListSet.from(seq)
    ListSet.from(ls).toSeq should equal(seq)
  }

  test("Can build an empty ListSet") {
    val seq = Seq()
    ListSet.from(seq) should be(empty)
    ListSet() should be(empty)
    ListSet().iterator should be(empty)
  }

  // Tests below are adapted from https://github.com/scala/scala/blob/2.13.x/test/junit/scala/collection/immutable/ListSetTest.scala

  test("t7445") {
    val s = ListSet(1, 2, 3, 4, 5)
    s.tail should equal(ListSet(2, 3, 4, 5))
  }

  test("hasCorrectBuilder") {
    val m = ListSet("a", "b", "c", "b", "d")
    m.toList should equal(List("a", "b", "c", "d"))
  }

  test("delete without stackoverflow") {
    val s = ListSet(1 to 50000: _*)
    try s - 25000
    catch {
      case _: StackOverflowError => fail("A stack overflow occurred")
    }
  }

  test("hasCorrectHeadTailLastInit") {
    val m = ListSet(1, 2, 3)
    m.head should equal(1)
    m.tail should equal(ListSet(2, 3))
    m.last should equal(3)
    m.init should equal(ListSet(1, 2))
  }

  test("hasCorrectAddRemove") {
    val m = ListSet(1, 2, 3)
    m + 4 should equal(ListSet(1, 2, 3, 4))
    m + 2 should equal(ListSet(1, 2, 3))
    m - 1 should equal(ListSet(2, 3))
    m - 2 should equal(ListSet(1, 3))
    m - 4 should equal(ListSet(1, 2, 3))
  }

  test("hasCorrectIterator") {
    val s = ListSet(1, 2, 3, 5, 4)
    s.iterator.toList should equal(List(1, 2, 3, 5, 4))
  }

  test("hasCorrectOrderAfterPlusPlus") {
    val foo = ListSet(1)
    var bar = foo ++ ListSet()
    bar.iterator.toList should equal(List(1))

    bar = foo ++ ListSet(1)
    bar.iterator.toList should equal(List(1))

    bar = foo ++ ListSet(2)
    bar.iterator.toList should equal(List(1, 2))

    bar = foo ++ ListSet(1, 2)
    bar.iterator.toList should equal(List(1, 2))

    bar = foo ++ ListSet(1, 2, 3)
    bar.iterator.toList should equal(List(1, 2, 3))

    bar = foo ++ ListSet(1, 2, 3, 4)
    bar.iterator.toList should equal(List(1, 2, 3, 4))

    bar = foo ++ ListSet(1, 2, 3, 4, 5)
    bar.iterator.toList should equal(List(1, 2, 3, 4, 5))

    bar = foo ++ ListSet(1, 2, 3, 4, 5, 6)
    bar.iterator.toList should equal(List(1, 2, 3, 4, 5, 6))

    foo ++ foo should equal(foo)
    foo ++ ListSet.empty should equal(foo)
    foo ++ Nil should equal(foo)
  }

  test("t12316 ++ is correctly ordered") {
    (ListSet(1, 2, 3, 42, 43, 44) ++ ListSet(10, 11, 12, 42, 43, 44, 27, 28, 29)).iterator.toList should equal(List(1,
      2, 3, 42, 43, 44, 10, 11, 12, 27, 28, 29))
  }

  test("smallPlusPlus1") {
    def check(l1: ListSet[Int], l2: ListSet[Int]) = {
      val expected = l1.iterator.toList ++ l2.iterator.filterNot(l1).toList
      val actual = (l1 ++ l2).iterator.toList
      actual should equal(expected)
    }

    for (
      start0 <- 0 until 6;
      end0 <- start0 until 6;
      start1 <- 0 until 6;
      end1 <- start1 until 6
    ) {
      val ls0 = ListSet((start0 until end0): _*)
      val ls1 = ListSet((start1 until end1): _*)
      check(ls0, ls1)
    }
  }

  test("smallPlusPlusAfter") {
    def check(l1: ListSet[Int], l2: ListSet[Int]) = {
      val expected = l1.iterator.toList ++ l2.iterator.filterNot(l1).toList
      val actual = (l1 ++ l2).iterator.toList
      actual should equal(expected)
    }

    for (
      start0 <- 0 until 9;
      end0 <- start0 until 9;
      start1 <- 10 until 19;
      end1 <- start1 until 19
    ) {
      val ls0 = ListSet((start0 until end0): _*)
      val ls1 = ListSet((start1 until end1): _*)
      check(ls0, ls1)
    }
  }

  private def twoSmallIntLists: Gen[(List[Int], List[Int])] = {
    for {
      xs <- Gen.listOf(Gen.chooseNum(1, 10))
      ys <- Gen.listOf(Gen.chooseNum(1, 10))
    } yield (xs, ys)
  }

  test("concat") {

    def concatTest(xs: List[Int], ys: List[Int]): Unit = {
      val listSet = ListSet.from(xs) concat ListSet.from(ys)
      val scalaListSet = immutable.ListSet.from(xs) concat immutable.ListSet.from(ys)
      listSet shouldEqual scalaListSet
    }

    forAll(twoSmallIntLists) { case (xs, ys) =>
      concatTest(xs, ys)
      concatTest(ys, xs)

      concatTest(xs, xs.empty)
      concatTest(xs.empty, xs)
    }
  }
}
