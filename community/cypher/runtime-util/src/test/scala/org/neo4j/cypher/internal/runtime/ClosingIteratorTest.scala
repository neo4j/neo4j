/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.GenTraversableOnce

class ClosingIteratorTest extends CypherFunSuite {

  test("closes resources when depleted") {
    val resource = new AutoCloseable {
      var closed = false
      override def close(): Unit = closed = true
    }
    val iter = ClosingIterator.empty.closing(resource)
    iter.hasNext shouldBe false
    resource.closed shouldBe true
  }

  test("closeMore when depleted") {
    val iter = new ClosingIterator[Int] {
      var closed = false
      override protected[this] def innerHasNext: Boolean = false
      override def closeMore(): Unit = closed = true
      override def next(): Int = 0
    }
    iter.hasNext shouldBe false
    iter.closed shouldBe true
  }

  // It is uttermost important to close resources only once:
  // Certain resources can be pooled. As soon as they get closed,
  // They will be returned to the pool, and can be picked up from somewhere else.
  // If the same resource gets then closed again, while it is already in reuse from a different location,
  // things break. This can happen even in a single-threaded environment.
  test("closes resources only once") {
    val resource = new AutoCloseable {
      var closeCount = 0
      override def close(): Unit = closeCount += 1
    }
    val iter = ClosingIterator.empty.closing(resource)
    iter.hasNext shouldBe false
    iter.close()
    iter.close()
    resource.closeCount shouldBe 1
  }

  test("closeMore only once") {
    val iter = new ClosingIterator[Int] {
      var closeCount = 0
      override protected[this] def innerHasNext: Boolean = false
      override def closeMore(): Unit = closeCount += 1
      override def next(): Int = 0
    }
    iter.hasNext shouldBe false
    iter.close()
    iter.close()
    iter.closeCount shouldBe 1
  }

  test("toIterator returns self") {
    val iter = ClosingIterator.empty
    //noinspection RedundantCollectionConversion
    iter.toIterator shouldBe theSameInstanceAs(iter)
  }

  test("flatMap explicit close closes current inner") {
    // given
    val outer = forever(0)
    val inner = forever(1)
    val flatMapped = outer.flatMap(_ => inner)
    // when
    Range(0, 10).foreach { _ =>
      flatMapped.hasNext
      flatMapped.next()
    }
    flatMapped.close()
    // then
    outer.closed shouldBe true
    inner.closed shouldBe true
  }

  test("flatMap inner iterators are closed when depleted") {
    // given
    val outer = forever(0)
    val inner1 = values(1)
    val inner2 = forever(2)
    val nextInner = Iterator(inner1, inner2)
    val flatMapped = outer.flatMap(_ => nextInner.next())
    // when
    flatMapped.hasNext
    flatMapped.next()
    flatMapped.hasNext
    // then
    inner1.closed shouldBe true
  }

  test("flatMap inner iterators of mixed types are closed when depleted and on explicit closed") {
    // given
    val outer = forever(0)
    val inner1: TestClosingIterator[Int] = values(1)
    val inner2: Option[Int] = Option.empty
    val inner3: Seq[Int] = Seq(3, 3)
    val inner4: TestClosingIterator[Int] = forever(4)
    val nextInner: Iterator[GenTraversableOnce[Int]] = Iterator(inner1, inner2, inner3, inner4)
    val flatMapped = outer.flatMap(_ => nextInner.next())
    // when
    flatMapped.hasNext
    flatMapped.next() shouldBe 1
    flatMapped.hasNext
    inner1.closed shouldBe true
    flatMapped.next() shouldBe 3
    flatMapped.hasNext
    flatMapped.next() shouldBe 3
    flatMapped.hasNext
    flatMapped.next() shouldBe 4
    inner4.closed shouldBe false
    flatMapped.close()
    inner4.closed shouldBe true
  }

  test("flatMap outer and inner iterators are closed when depleted") {
    // given
    val outer = values(1)
    val inner = values(1)
    val flatMapped = outer.flatMap(_ => inner)
    // when
    flatMapped.hasNext
    flatMapped.next()
    flatMapped.hasNext
    // then
    outer.closed shouldBe true
    inner.closed shouldBe true
  }

  test("flatMap with empty outer should not call inner lambda") {
    // given
    val outer = ClosingIterator.empty
    val flatMapped = outer.flatMap(_ => fail("should not call inner lambda"))
    // when
    flatMapped.hasNext
  }

  test("filter should close when depleted") {
    // given
    val outer = values(1)
    val filtered = outer.filter(_ => false)
    // when
    filtered.hasNext
    // then
    outer.closed shouldBe true
  }

  test("filter should not close in the middle") {
    // given
    val outer = values(1, 2, 3, 4, 5)
    val filtered = outer.filter((i: Int) => i % 2 == 0)
    // when
    filtered.hasNext
    filtered.next()
    outer.closed shouldBe false
    filtered.hasNext
    filtered.next()
    outer.closed shouldBe false
    filtered.hasNext
    outer.closed shouldBe true
  }

  test("map should close on explicit close") {
    // given
    val outer = values(1, 2, 3)
    val mapped = outer.map(i => i + 1)
    // when
    mapped.hasNext
    mapped.next()
    mapped.close()
    // then
    outer.closed shouldBe true
  }

  test("++ closes when depleted and returns correct results") {
    // given
    val first = values(1, 2)
    val second = values(3, 4)
    val concatted = first ++ second
    // when
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 1
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 2
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 3
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 4
    concatted.hasNext shouldBe false
    // then
    first.closed shouldBe true
    second.closed shouldBe true
  }

  test("++ when first is empty") {
    // given
    val first = values()
    val second = values(3, 4)
    val concatted = first ++ second
    // when
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 3
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 4
    concatted.hasNext shouldBe false
    // then
    first.closed shouldBe true
    second.closed shouldBe true
  }

  test("++ when second is empty") {
    // given
    val first = values(3, 4)
    val second = values()
    val concatted = first ++ second
    // when
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 3
    concatted.hasNext shouldBe true
    concatted.next() shouldBe 4
    concatted.hasNext shouldBe false
    // then
    first.closed shouldBe true
    second.closed shouldBe true
  }

  test("++ closes on explicit close while iterating over first") {
    // given
    val first = values(1, 2)
    val second = values(3, 4)
    val concatted = first ++ second
    // when
    concatted.next()
    concatted.close()
    // then
    first.closed shouldBe true
    second.closed shouldBe true
  }

  test("++ closes on explicit close while iterating over second") {
    // given
    val first = values(1, 2)
    val second = values(3, 4)
    val concatted = first ++ second
    // when
    concatted.next()
    concatted.next()
    concatted.next()
    concatted.close()
    // then
    first.closed shouldBe true
    second.closed shouldBe true
  }

  test("++ close should not regenerate iterator") {
    // given
    // Inlining so that the second argument is call-by-name
    var c = 0
    def countingValues[T](v: T*) = {
      c += 1
      values(v: _*)
    }

    val concatted = values(1) ++ countingValues(2)

    // when
    // exhaust
    concatted.next()
    concatted.next()
    concatted.hasNext should be(false)
    // close
    concatted.close()

    // then
    concatted.hasNext should /* still */ be(false)
    c shouldBe 1
  }

  test("++ close should not eagerize second iterator") {
    // given
    // Inlining so that the second argument is call-by-name
    var created = false
    def rememberingValues[T](v: T*) = {
      created = true
      values(v: _*)
    }

    // when
    val concatted = values(1) ++ rememberingValues(2)

    // then
    created shouldBe false

    // and when
    concatted.next()
    concatted.hasNext

    // then
    created shouldBe true
  }

  test("single returns one element") {
    val single = ClosingIterator.single(1)
    single.hasNext shouldBe true
    single.next() shouldBe 1
    single.hasNext shouldBe false
  }

  abstract class TestClosingIterator[+T] extends ClosingIterator[T] {
    var closed = false
    override def closeMore(): Unit = closed = true
  }

  private def values[T](values: T*): TestClosingIterator[T] = new TestClosingIterator[T] {
    private val inner = Iterator(values: _*)
    override protected[this] def innerHasNext: Boolean = inner.hasNext
    override def next(): T = inner.next()
  }

  private def forever[T](value: T): TestClosingIterator[T] = new TestClosingIterator[T] {
    override protected[this] def innerHasNext: Boolean = true
    override def next(): T = value
  }

}
