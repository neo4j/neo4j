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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.collection.primitive.{PrimitiveLongIterable, PrimitiveLongSet}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LazyGroupingIteratorTest extends CypherFunSuite {

  test("should produce empty iterator from empty iterator") {
    new LazyGroupingRowIterator() should be(empty)
  }

  test("should produce single row from input") {
    new LazyGroupingRowIterator(new Row("foo", 1)).toList should equal(List(new Row("foo", 1)))
  }

  test("should update state per key group in input") {
    // given
    val iterator = new LazyGroupingRowIterator(
      new Row("a", 1), new Row("a", 3),
      new Row("b", 3), new Row("b", 2),
      new Row("c", 4))

    // then

    iterator.state shouldBe null

    iterator.next() should equal(new Row("a", 1))
    val stateA = iterator.state
    asScalaSet(stateA) should equal(Set(1, 3))

    iterator.next() should equal(new Row("a", 3))
    iterator.state should be theSameInstanceAs stateA
    asScalaSet(stateA) should equal(Set(1, 3))

    iterator.next() should equal(new Row("b", 3))
    val stateB = iterator.state
    stateB shouldNot be theSameInstanceAs stateA
    asScalaSet(stateB) should equal(Set(2, 3))

    iterator.next() should equal(new Row("b", 2))
    iterator.state should be theSameInstanceAs stateB
    asScalaSet(stateB) should equal(Set(2, 3))

    iterator.next() should equal(new Row("c", 4))
    val stateC = iterator.state
    stateC shouldNot be theSameInstanceAs stateA
    stateC shouldNot be theSameInstanceAs stateB
    asScalaSet(stateC) should equal(Set(4))

    iterator.hasNext shouldBe false
  }

  test("should let null through, but not include it in the state") {
    // given
    val iterator = new LazyGroupingRowIterator(new Row("a", 1), new Row("a", None), new Row("a", 2))

    iterator.next() should equal(new Row("a", 1))
    val state = iterator.state
    iterator.next() should equal(new Row("a"))
    iterator.next() should equal(new Row("a", 2))
    iterator.hasNext shouldBe false
    asScalaSet(state) should equal(Set(1, 2))
  }

  case class Row(key: Any, value: Option[Long]) {
    def this(key: Any, value: Long) = this(key, Some(value))

    def this(key: Any) = this(key, None)
  }

  class LazyGroupingRowIterator(rows: Row*) extends LazyGroupingIterator[Row](rows.iterator) {
    var state: PrimitiveLongSet = null

    override def setState(state: PrimitiveLongSet) = {
      this.state = state
    }

    override def getValue(row: Row) = row.value

    override def getKey(row: Row) = row.key
  }

  def asScalaSet(in: PrimitiveLongIterable): Set[Long] = {
    val builder = Set.newBuilder[Long]
    val iter = in.iterator()
    while (iter.hasNext) {
      builder += iter.next()
    }
    builder.result()
  }
}
