/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import scala.util.Random

class RowsMatcherTest extends CypherFunSuite with TestName {

  private val NO_ROWS = IndexedSeq[Array[AnyValue]]()
  private val NO_PARTIAL_ROWS = IndexedSeq[IndexedSeq[Array[AnyValue]]]()

  test("AnyRowsMatcher") {
    AnyRowsMatcher.matchesRaw(Array[String](), NO_ROWS) should be(true)
    AnyRowsMatcher.matchesRaw(Array("a"), NO_ROWS) should be(true)
    AnyRowsMatcher.matchesRaw(Array("a"), Array(row(1))) should be(true)
    AnyRowsMatcher.matchesRaw(
      Array("X", "Y", "Z"),
      Array(
        row(1, 0, 4),
        row(4, 0, 4),
        row(1, 2, 4),
        row(2, 0, 4)
      )
    ) should be(true)
  }

  test("NoRowsMatcher") {
    NoRowsMatcher.matchesRaw(Array[String](), NO_ROWS) should be(true)
    NoRowsMatcher.matchesRaw(Array("a"), NO_ROWS) should be(true)
    NoRowsMatcher.matchesRaw(Array("a"), Array(row(1))) should be(false)
    NoRowsMatcher.matchesRaw(
      Array("X", "Y", "Z"),
      Array(
        row(1, 0, 4),
        row(4, 0, 4),
        row(1, 2, 4),
        row(2, 0, 4)
      )
    ) should be(false)
  }

  test("EqualInAnyOrder") {
    EqualInAnyOrder(NO_ROWS).matchesRaw(Array[String](), NO_ROWS) should be(true)
    EqualInAnyOrder(NO_ROWS).matchesRaw(Array("a"), NO_ROWS) should be(true)
    EqualInAnyOrder(NO_ROWS).matchesRaw(Array("a"), Array(row(1))) should be(false)
    EqualInAnyOrder(Array(row(1))).matchesRaw(Array("a"), Array(row(1))) should be(true)
    EqualInAnyOrder(Array(row(2))).matchesRaw(Array("a"), Array(row(1))) should be(false)
    EqualInAnyOrder(Array(row(1, 0, 4))).matchesRaw(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(true)
    EqualInAnyOrder(Array(row(1, 0, 5))).matchesRaw(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(false)

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4)
    )

    EqualInAnyOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows) should be(true)
    EqualInAnyOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows.tail :+ rows.head) should be(true)
    EqualInAnyOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows.reverse) should be(true)
    EqualInAnyOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows :+ rows.head) should be(false)
  }

  test("EqualInOrder") {
    EqualInOrder(NO_ROWS).matchesRaw(Array[String](), NO_ROWS) should be(true)
    EqualInOrder(NO_ROWS).matchesRaw(Array("a"), NO_ROWS) should be(true)
    EqualInOrder(NO_ROWS).matchesRaw(Array("a"), Array(row(1))) should be(false)
    EqualInOrder(Array(row(1))).matchesRaw(Array("a"), Array(row(1))) should be(true)
    EqualInOrder(Array(row(2))).matchesRaw(Array("a"), Array(row(1))) should be(false)
    EqualInOrder(Array(row(1, 0, 4))).matchesRaw(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(true)
    EqualInOrder(Array(row(1, 0, 5))).matchesRaw(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(false)

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4)
    )

    EqualInAnyOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows) should be(true)
    EqualInOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows.tail :+ rows.head) should be(false)
    EqualInOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows.reverse) should be(false)
    EqualInOrder(rows).matchesRaw(Array("X", "Y", "Z"), rows :+ rows.head) should be(false)
  }

  test("EqualInAnyOrder.matches") {
    val rows = (0 until 100).map(i => row(i * 100))
    val rowsAt21 = (0 until 7).map(i => row(2100 + i))

    EqualInAnyOrder(rows).matches(Array("X"), rows) should be(RowsMatch)

    EqualInAnyOrder(rows).matches(Array("X"), rows.take(73) ++ rowsAt21 ++ rows.drop(87)) should be(
      RowsDontMatch(
        """Expected 100 rows, but got 93 rows
          |    ... 22 matching rows ...
          | + Int(2100)
          | + Int(2101)
          | + Int(2102)
          | + Int(2103)
          | + Int(2104)
          | + Int(2105)
          | + Int(2106)
          |    ... 51 matching rows ...
          | - Int(7300)
          | - Int(7400)
          | - Int(7500)
          | - Int(7600)
          | - Int(7700)
          | - Int(7800)
          | - Int(7900)
          | - Int(8000)
          | - Int(8100)
          | - Int(8200)
          | - Int(8300)
          | - Int(8400)
          | - Int(8500)
          | - Int(8600)
          |    ... 13 matching rows ...
          |""".stripMargin.replace("\r\n", "\n")
      )
    )

    EqualInAnyOrder(rows).matches(Array("X"), rows.slice(1, 99) :+ row(-1) :+ row(999999)) should be(
      RowsDontMatch(
        """Expected 100 rows, and got 100 rows
          | + Int(-1)
          | - Int(0)
          |    ... 98 matching rows ...
          | - Int(9900)
          | + Int(999999)
          |""".stripMargin.replace("\r\n", "\n")
      )
    )
  }

  test("EqualInPartialOrder") {
    // should behave the same as EqualInAnyOrder when there is only one group

    EqualInPartialOrder(NO_PARTIAL_ROWS).matchesRaw(Array[String](), NO_ROWS) should be(true)
    EqualInPartialOrder(NO_PARTIAL_ROWS).matchesRaw(Array("a"), NO_ROWS) should be(true)
    EqualInPartialOrder(NO_PARTIAL_ROWS).matchesRaw(Array("a"), Array(row(1))) should be(false)
    EqualInPartialOrder(IndexedSeq(Array(row(1)))).matchesRaw(Array("a"), Array(row(1))) should be(true)
    EqualInPartialOrder(IndexedSeq(Array(row(2)))).matchesRaw(Array("a"), Array(row(1))) should be(false)

    EqualInPartialOrder(IndexedSeq(Array(row(1, 0, 4)))).matchesRaw(
      Array("X", "Y", "Z"),
      Array(row(1, 0, 4))
    ) should be(true)
    EqualInPartialOrder(IndexedSeq(Array(row(1, 0, 5)))).matchesRaw(
      Array("X", "Y", "Z"),
      Array(row(1, 0, 4))
    ) should be(false)

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4)
    )

    val singleRowsGroup: IndexedSeq[IndexedSeq[Array[AnyValue]]] = IndexedSeq(rows)

    EqualInPartialOrder(singleRowsGroup).matchesRaw(Array("X", "Y", "Z"), rows) should be(true)
    EqualInPartialOrder(singleRowsGroup).matchesRaw(Array("X", "Y", "Z"), rows.tail :+ rows.head) should be(true)
    EqualInPartialOrder(singleRowsGroup).matchesRaw(Array("X", "Y", "Z"), rows.reverse) should be(true)
    EqualInPartialOrder(singleRowsGroup).matchesRaw(Array("X", "Y", "Z"), rows :+ rows.head) should be(false)

    // should work with single row groups

    val oneRowGroups: IndexedSeq[IndexedSeq[Array[AnyValue]]] = IndexedSeq(
      Array(
        row(1, 0, 4)
      ),
      Array(
        row(4, 0, 4)
      ),
      Array(
        row(4, 0, 4)
      )
    )

    EqualInPartialOrder(oneRowGroups).matchesRaw(Array("X", "Y", "Z"), oneRowGroups.flatten) should be(true)
    EqualInPartialOrder(oneRowGroups).matchesRaw(Array("X", "Y", "Z"), oneRowGroups.take(2).flatten) should be(false)

    // should work with multi-row groups

    val multiRowGroups: IndexedSeq[IndexedSeq[Array[AnyValue]]] = IndexedSeq(
      Array(
        row(4, 0, 1)
      ),
      Array(
        row(4, 0, 2),
        row(4, 0, 3)
      ),
      Array(
        row(4, 0, 4),
        row(4, 0, 5)
      )
    )

    EqualInPartialOrder(multiRowGroups).matchesRaw(Array("X", "Y", "Z"), multiRowGroups.flatten) should be(true)
    EqualInPartialOrder(multiRowGroups).matchesRaw(Array("X", "Y", "Z"), multiRowGroups.take(2).flatten) should be(
      false
    )

    val oneRowMovedAcrossGroupBoundary: IndexedSeq[Array[AnyValue]] = Array(
      row(4, 0, 1),
      row(4, 0, 2),
      row(4, 0, 4),
      row(4, 0, 3), // row moved into "next group"
      row(4, 0, 5)
    )

    EqualInPartialOrder(multiRowGroups).matchesRaw(Array("X", "Y", "Z"), oneRowMovedAcrossGroupBoundary) should be(
      false
    )

    val rowsMovedWithinGroupBoundaries: IndexedSeq[Array[AnyValue]] = Array(
      row(4, 0, 1),
      row(4, 0, 3),
      row(4, 0, 2),
      row(4, 0, 5),
      row(4, 0, 4)
    )

    EqualInPartialOrder(multiRowGroups).matchesRaw(Array("X", "Y", "Z"), rowsMovedWithinGroupBoundaries) should be(true)
  }

  test("EqualInPartialOrder.matches") {
    // should behave the same as EqualInAnyOrder when there is only one group

    val rows = (0 until 100).map(i => row(i * 100))
    val rowsAt21 = (0 until 7).map(i => row(2100 + i))

    val singleRowsGroup: IndexedSeq[IndexedSeq[Array[AnyValue]]] = IndexedSeq(rows)

    EqualInPartialOrder(singleRowsGroup).matches(Array("X"), rows) should be(RowsMatch)

    EqualInPartialOrder(singleRowsGroup).matches(Array("X"), rows.take(73) ++ rowsAt21 ++ rows.drop(87)) should be(
      RowsDontMatch(
        """    ... 22 matching rows ...
          | + Int(2100)
          | + Int(2101)
          | + Int(2102)
          | + Int(2103)
          | + Int(2104)
          | + Int(2105)
          | + Int(2106)
          |    ... 51 matching rows ...
          | - Int(7300)
          | - Int(7400)
          | - Int(7500)
          | - Int(7600)
          | - Int(7700)
          | - Int(7800)
          | - Int(7900)
          | - Int(8000)
          | - Int(8100)
          | - Int(8200)
          | - Int(8300)
          | - Int(8400)
          | - Int(8500)
          | - Int(8600)
          |    ... 13 matching rows ...
          |""".stripMargin.replace("\r\n", "\n")
      )
    )

    EqualInPartialOrder(singleRowsGroup).matches(Array("X"), rows.slice(1, 99) :+ row(-1) :+ row(999999)) should be(
      RowsDontMatch(
        """ + Int(-1)
          | - Int(0)
          |    ... 98 matching rows ...
          | - Int(9900)
          | + Int(999999)
          |""".stripMargin.replace("\r\n", "\n")
      )
    )

    // should work with multi-row groups

    val expectedGroupedRows: IndexedSeq[IndexedSeq[Array[AnyValue]]] = IndexedSeq(
      Array(
        row(1),
        row(2)
      ),
      Array(
        row(3),
        row(4),
        row(5)
      ),
      Array(
        row(6)
      )
    )

    val actualGroupedRowsIsLonger: IndexedSeq[Array[AnyValue]] = Array(
      row(0), // extra
      // ---
      row(2),
      row(1),
      // ---
      row(4),
      row(3),
      row(5),
      row(5), // extra
      // ---
      // row(6), // removed
      row(7) // extra
    )

    EqualInPartialOrder(expectedGroupedRows).matches(Array("X"), actualGroupedRowsIsLonger) should be(RowsDontMatch(
      """ + Int(0)
        | - Int(1)
        |    ... 1 matching rows ...
        |    --- <group boundary> ---
        | + Int(1)
        |    ... 2 matching rows ...
        | - Int(5)
        |    --- <group boundary> ---
        | + Int(5)
        | - Int(6)
        | + Int(5)
        | + Int(7)
        |""".stripMargin.replace("\r\n", "\n")
    ))

    val actualGroupedRowsIsShorter: IndexedSeq[Array[AnyValue]] = Array(
      row(0), // extra
      // ---
      row(2),
      row(1),
      // ---
      row(4),
      row(3)
    )

    EqualInPartialOrder(expectedGroupedRows).matches(Array("X"), actualGroupedRowsIsShorter) should be(RowsDontMatch(
      """ + Int(0)
        | - Int(1)
        |    ... 1 matching rows ...
        |    --- <group boundary> ---
        | + Int(1)
        |    ... 2 matching rows ...
        | - Int(5)
        |    --- <group boundary> ---
        | - Int(6)
        |""".stripMargin.replace("\r\n", "\n")
    ))
  }

  test("listInAnyOrder basic") {
    val random = new Random()

    val elements = (0 until 100).map(i => Values.intValue(i))
    val ordered = VirtualValues.list(elements.toArray: _*)
    val shuffled = VirtualValues.list(random.shuffle(elements).toArray: _*)

    val got = IndexedSeq(Array[AnyValue](ordered))
    val expected = IndexedSeq(Array[AnyValue](shuffled))

    EqualInAnyOrder(got).matchesRaw(Array("x"), expected) should be(false)
    EqualInAnyOrder(got, listInAnyOrder = true).matchesRaw(Array("x"), expected) should be(true)
  }

  test("listInAnyOrder nested") {
    val random = new Random()
    def nestedLists(shuffle: Boolean): ListValue = {
      val outerElements =
        (0 until 100).map(_ => {
          val rawElements = (0 until 10).map(j => Values.intValue(j))
          val elements = if (shuffle) random.shuffle(rawElements) else rawElements
          VirtualValues.list(elements.toArray: _*)
        })
      VirtualValues.list(outerElements.toArray: _*)
    }

    val ordered = nestedLists(shuffle = false)
    val shuffled = nestedLists(shuffle = true)

    val got = IndexedSeq(Array[AnyValue](ordered))
    val expected = IndexedSeq(Array[AnyValue](shuffled))

    EqualInAnyOrder(got).matchesRaw(Array("x"), expected) should be(false)
    EqualInAnyOrder(got, listInAnyOrder = true).matchesRaw(Array("x"), expected) should be(true)
  }

  test("GroupBy") {
    assertFailNoRows(new GroupBy(None, None, "a"))
    assertOkSingleRow(new GroupBy(None, None, "a"))

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 2, 4)
    )

    new GroupBy(None, None, "a").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(None, None, "b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(None, None, "c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
  }

  test("GroupBy multi") {
    val rows = Array(
      row(1, 1),
      row(1, 1),
      row(4, 0),
      row(1, 2),
      row(2, 2),
      row(2, 2),
      row(4, 1)
    )

    new GroupBy(None, None, "a", "b").matchesRaw(Array("a", "b"), rows) should be(true)
    new GroupBy(None, None, "b", "a").matchesRaw(Array("a", "b"), rows) should be(true)
    new GroupBy(None, None, "b", "a").matchesRaw(Array("a", "b"), rows :+ rows.head) should be(false)
    new GroupBy(None, None, "b", "a").matchesRaw(Array("a", "b"), rows.last +: rows) should be(false)
  }

  test("GroupBy should assert on group size") {

    val rows = Array(
      row(1, 0, 4),
      row(1, 0, 4),
      row(1, 0, 4),
      row(1, 2, 4),
      row(3, 2, 4),
      row(3, 2, 4)
    )

    new GroupBy(None, Some(1), "a").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(None, Some(4), "a").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(None, Some(1), "b").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(None, Some(3), "b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(None, Some(4), "b").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(None, Some(6), "c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
  }

  test("GroupBy should assert on number of groups") {

    val rows = Array(
      row(1, 0, 4),
      row(1, 0, 4),
      row(1, 0, 4),
      row(1, 2, 4),
      row(3, 2, 4),
      row(3, 2, 4)
    )

    new GroupBy(Some(1), Some(3), "b").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(Some(2), Some(3), "b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(Some(3), Some(3), "b").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new GroupBy(Some(1), Some(6), "c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(Some(2), Some(6), "c").matchesRaw(Array("a", "b", "c"), rows) should be(false)
  }

  test("Ascending") {
    assertFailNoRows(new Ascending("a"))
    assertOkSingleRow(new Ascending("a"))

    val rows = Array(
      row(1, 0, 1),
      row(4, 0, 2),
      row(4, 0, 3),
      row(1, 2, 4),
      row(2, 2, 5)
    )

    new Ascending("a").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new Ascending("b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
  }

  test("Descending") {
    assertFailNoRows(new Descending("a"))
    assertOkSingleRow(new Descending("a"))

    val rows = Array(
      row(5, 1, 5),
      row(4, 1, 4),
      row(4, 1, 3),
      row(1, 0, 2),
      row(2, 0, 1)
    )

    new Descending("a").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new Descending("b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Descending("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
  }

  test("ComplexOrder") {
    val rows = Array(
      row(5, 1, 4),
      row(4, 1, 4),
      row(4, 1, 5),
      row(4, 0, 5),
      row(2, 6, 5),
      row(2, 5, 5),
      row(2, 4, 5),
      row(2, 3, 6),
      row(2, 2, 6),
      row(2, 1, 6)
    )

    new GroupBy(None, None, "a").desc("b").asc("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(None, None, "c").desc("a").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new GroupBy(None, None, "c").groupBy("b").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").desc("a").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").desc("b").matchesRaw(Array("a", "b", "c"), rows) should be(false)
    new Descending("a").desc("b").asc("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
  }

  test("ComplexOrder2") {
    val rows = Array(
      row(1, 1, 1),
      row(1, 1, 2),
      row(1, 2, 1),
      row(1, 2, 2),
      row(1, 3, 1),
      row(1, 3, 2),
      row(2, 1, 1),
      row(2, 1, 2),
      row(2, 2, 1),
      row(2, 2, 2),
      row(2, 3, 1),
      row(2, 3, 2),
      row(3, 1, 1),
      row(3, 1, 2),
      row(3, 2, 1),
      row(3, 2, 2),
      row(3, 3, 1),
      row(3, 3, 2)
    )

    new GroupBy(None, None, "a").groupBy("b").groupBy("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Ascending("a").asc("b").asc("c").matchesRaw(Array("a", "b", "c"), rows) should be(true)
    new Descending("a").desc("b").desc("c").matchesRaw(Array("a", "b", "c"), rows.reverse) should be(true)
  }

  private def assertFailNoRows(columnAMatcher: RowOrderMatcher): Unit = {
    columnAMatcher.matchesRaw(Array[String](), NO_ROWS) should be(false)
    columnAMatcher.matchesRaw(Array("a"), NO_ROWS) should be(false)
  }

  private def assertOkSingleRow(columnAMatcher: RowOrderMatcher): Unit = {
    columnAMatcher.matchesRaw(Array("a"), Array(row(1))) should be(true)
    columnAMatcher.matchesRaw(Array("a", "b", "c"), Array(row(1, 0, 4))) should be(true)
    columnAMatcher.matchesRaw(Array("x", "a", "c"), Array(row(1, 0, 4))) should be(true)
    intercept[IllegalArgumentException](columnAMatcher.matchesRaw(Array("X"), Array(row(1))))
  }

  private def row(values: Any*): Array[AnyValue] =
    values.map(Values.of).toArray
}
