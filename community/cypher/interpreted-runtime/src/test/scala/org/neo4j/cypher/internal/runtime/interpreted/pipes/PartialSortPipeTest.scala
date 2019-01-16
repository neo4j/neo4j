/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.junit.Assert.assertEquals
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable.{Map => MutableMap}

class PartialSortPipeTest extends CypherFunSuite with MockitoSugar {

  test("empty input gives empty output") {
    val source = new FakePipe(List())
    val sortPipe = PartialSortPipe(source, List(Ascending("x")), List(Ascending("y")))()

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList)
  }

  test("partial sort with one column already sorted with only one distinct value") {
    val list = List(
      MutableMap("x" -> 3, "y" -> 1),
      MutableMap("x" -> 3, "y" -> 3),
      MutableMap("x" -> 3, "y" -> 2))
    val source = new FakePipe(list)
    val sortPipe = PartialSortPipe(source, List(Ascending("x")), List(Ascending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 2),
      Map("x" -> 3, "y" -> 3)))
  }

  test("partial sort with one column already sorted") {
    val list = List(
      MutableMap("x" -> 3, "y" -> 1),
      MutableMap("x" -> 3, "y" -> 3),
      MutableMap("x" -> 3, "y" -> 2),
      MutableMap("x" -> 5, "y" -> 9),
      MutableMap("x" -> 5, "y" -> 9),
      MutableMap("x" -> 5, "y" -> 0),
      MutableMap("x" -> 5, "y" -> 7)
    )
    val source = new FakePipe(list)
    val sortPipe = PartialSortPipe(source, List(Ascending("x")), List(Ascending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 2),
      Map("x" -> 3, "y" -> 3),
      Map("x" -> 5, "y" -> 0),
      Map("x" -> 5, "y" -> 7),
      Map("x" -> 5, "y" -> 9),
      Map("x" -> 5, "y" -> 9)
    ))
  }

  test("partial sort with two sorted and two unsorted columns") {
    val list = List(
      MutableMap("x" -> 3, "y" -> 3, "z" -> 0, "a" -> 1),
      MutableMap("x" -> 3, "y" -> 1, "z" -> 9, "a" -> 2),
      MutableMap("x" -> 3, "y" -> 1, "z" -> 3, "a" -> 3),
      MutableMap("x" -> 5, "y" -> 5, "z" -> 4, "a" -> 0),
      MutableMap("x" -> 5, "y" -> 5, "z" -> 2, "a" -> 0),
      MutableMap("x" -> 5, "y" -> 5, "z" -> 2, "a" -> 6),
      MutableMap("x" -> 5, "y" -> 0, "z" -> 2, "a" -> 0)
    )
    val source = new FakePipe(list)
    val sortPipe = PartialSortPipe(source, List(Ascending("x"), Descending("y")), List(Ascending("z"), Descending("a")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 3, "y" -> 3, "z" -> 0, "a" -> 1),
      Map("x" -> 3, "y" -> 1, "z" -> 3, "a" -> 3),
      Map("x" -> 3, "y" -> 1, "z" -> 9, "a" -> 2),
      Map("x" -> 5, "y" -> 5, "z" -> 2, "a" -> 6),
      Map("x" -> 5, "y" -> 5, "z" -> 2, "a" -> 0),
      Map("x" -> 5, "y" -> 5, "z" -> 4, "a" -> 0),
      Map("x" -> 5, "y" -> 0, "z" -> 2, "a" -> 0)
    ))
  }

  test( "should handle null values" ) {
    val list = List(
      MutableMap("x" -> 3, "y" -> 1),
      MutableMap("x" -> 3, "y" -> null),
      MutableMap("x" -> 3, "y" -> 2),
      MutableMap("x" -> 5, "y" -> null),
      MutableMap("x" -> 5, "y" -> 9),
      MutableMap("x" -> null, "y" -> 0),
      MutableMap("x" -> null, "y" -> null)
    )
    val source = new FakePipe(list)
    val sortPipe = PartialSortPipe(source, List(Ascending("x")), List(Ascending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 2),
      Map("x" -> 3, "y" -> Values.NO_VALUE),
      Map("x" -> 5, "y" -> 9),
      Map("x" -> 5, "y" -> Values.NO_VALUE),
      Map("x" -> Values.NO_VALUE, "y" -> 0),
      Map("x" -> Values.NO_VALUE, "y" -> Values.NO_VALUE)
    ))
  }

  test("partial sort is lazy") {
    val list = List(
      MutableMap("x" -> 3, "y" -> 1),
      MutableMap("x" -> 3, "y" -> 2),
      MutableMap("x" -> 5, "y" -> 9),
      MutableMap("x" -> 5, "y" -> 9),
      MutableMap("x" -> 5, "y" -> 0),
      MutableMap("x" -> 5, "y" -> 7),
      MutableMap("x" -> 6, "y" -> 1),
      MutableMap("x" -> 6, "y" -> 1)
    )
    val source = new FakePipe(list)

    val sortPipe = PartialSortPipe(source, List(Ascending("x")), List(Ascending("y")))()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.next() // 3, 1
    source.numberOfPulledRows should be(3)

    iterator.next() // 3, 2
    source.numberOfPulledRows should be(3)

    iterator.next() // 5, 0
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 7
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 9
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 9
    source.numberOfPulledRows should be(7)

    iterator.next() // 6, 1
    source.numberOfPulledRows should be(8)

    iterator.next() // 6, 1
    source.numberOfPulledRows should be(8)

    iterator.hasNext should be(false)
  }

}
