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

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.runtime.spec.RowDiffStringBuilder.PARTIALLY_ORDERED_GROUP_SEPARATOR
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues
import org.neo4j.values.SequenceValue
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.matchers.Matcher

import java.util.Objects

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Rows {
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)

  def pretty(a: IndexedSeq[Array[AnyValue]]): String = {
    val sb = new StringBuilder
    if (a.isEmpty) {
      sb ++= "<NO ROWS>"
    } else {
      sb ++= s"<${a.size} rows>\n"
    }

    // There is a bug in IntelliJ that falsely displays a test as green if there is too much output in certain cases. (It is still red in maven though)
    // This .take(1000) is too avoid that situation
    for (row <- a.take(1000))
      sb ++= prettyRow(row)
    if (a.length > 1000) {
      sb ++= "...\n"
    }
    sb.result()
  }

  def prettyRow(row: Array[AnyValue]): String = {
    row.map(value => Objects.toString(value)).mkString("", ", ", "\n")
  }
}

sealed trait RowMatchResult
case object RowsMatch extends RowMatchResult
case class RowsDontMatch(errorMessage: String) extends RowMatchResult

trait RowsMatcher {

  def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): RowMatchResult =
    if (matchesRaw(columns, rows)) {
      RowsMatch
    } else {
      RowsDontMatch(errorMessage(rows))
    }

  def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean =
    matches(columns, rows) == RowsMatch
  protected def formatRows(rows: IndexedSeq[Array[AnyValue]]): String

  protected def errorMessage(rows: IndexedSeq[Array[AnyValue]]): String =
    s"""Expected:
       |
       |$this
       |
       |but got
       |
       |${formatRows(rows)}""".stripMargin
}

object AnyRowsMatcher extends RowsMatcher {
  override def toString: String = "<ANY ROWS>"
  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = true
  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

object NoRowsMatcher extends RowsMatcher {
  override def toString: String = "<NO ROWS>"
  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = rows.isEmpty
  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

abstract class EqualRowsMatcher(listInAnyOrder: Boolean) extends RowsMatcher {

  protected def matchSorted(expRowsRaw: IndexedSeq[ListValue], gotRowsRaw: IndexedSeq[ListValue]): RowMatchResult = {
    matchSorted(expRowsRaw, gotRowsRaw, new RowDiffStringBuilder())
  }

  protected def matchSorted(
    expRows: IndexedSeq[ListValue],
    gotRows: IndexedSeq[ListValue],
    diffString: RowDiffStringBuilder
  ): RowMatchResult = {

    var expI = 0
    var gotI = 0
    var matchStreak = 0

    while (expI < expRows.size && gotI < gotRows.size) {
      val exp = expRows(expI)
      val got = gotRows(gotI)
      val comp = AnyValues.COMPARATOR.compare(got, exp)
      if (comp == 0) {
        matchStreak += 1
        expI += 1
        gotI += 1
      } else {
        if (matchStreak > 0) {
          diffString.onMatchStreak(matchStreak)
          matchStreak = 0
        }
        if (comp < 0) {
          diffString.onExtraRow(got)
          gotI += 1
        } else { // comp > 0
          diffString.onMissingRow(exp)
          expI += 1
        }
      }
    }

    if (matchStreak > 0) {
      diffString.onMatchStreak(matchStreak)
    }

    while (expI < expRows.size) {
      val exp = expRows(expI)
      diffString.onMissingRow(exp)
      expI += 1
    }

    while (gotI < gotRows.size) {
      val got = gotRows(gotI)
      diffString.onExtraRow(got)
      gotI += 1
    }

    if (matchStreak == expRows.size && matchStreak == gotRows.size)
      RowsMatch
    else {
      val expRowCount = expRows.size
      val gotRowCount = gotRows.size
      val conjunction = if (expRowCount == gotRowCount) "and" else "but"
      RowsDontMatch(
        s"Expected $expRowCount rows, $conjunction got $gotRowCount rows\n${diffString.result}"
      )
    }
  }

  protected def sortIfNeeded(sortRows: Boolean)(rows: IndexedSeq[Array[AnyValue]]): IndexedSeq[ListValue] = {
    val rowsWithSortedLists = rows
      .map {
        if (listInAnyOrder) row => row.map(_.map(SortListValueMapper))
        else row => row
      }
      .map(row => VirtualValues.list(row: _*))
    if (sortRows) {
      rowsWithSortedLists.sorted(Rows.ANY_VALUE_ORDERING)
    } else {
      rowsWithSortedLists
    }
  }
}

case class EqualInAnyOrder(expected: IndexedSeq[Array[AnyValue]], listInAnyOrder: Boolean = false)
    extends EqualRowsMatcher(listInAnyOrder) {
  override def toString: String = formatRows(expected) + " in any order"

  private def sort = sortIfNeeded(sortRows = true)(_)

  override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): RowMatchResult = {
    matchSorted(sort(expected), sort(rows))
  }

  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String =
    Rows.pretty(rows.map(row => VirtualValues.list(row: _*)).sorted(Rows.ANY_VALUE_ORDERING).map(l => l.asArray()))
}

case class EqualInPartialOrder(expecteds: IndexedSeq[IndexedSeq[Array[AnyValue]]], listInAnyOrder: Boolean = false)
    extends EqualRowsMatcher(listInAnyOrder) {

  override def toString: String =
    expecteds.map(formatRows).mkString(RowDiffStringBuilder.PARTIALLY_ORDERED_GROUP_SEPARATOR) + " in partial order"

  private def sort = sortIfNeeded(sortRows = true)(_)

  override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): RowMatchResult = {
    val diffString = new RowDiffStringBuilder()
    var success = true
    var i = 0
    var rowsOffset = 0
    while (i < expecteds.length) {
      if (i > 0) {
        diffString.onEndGroup()
      }
      val expectedRows = expecteds(i)
      checkOnlyWhenAssertionsAreEnabled(expectedRows.nonEmpty, "Matcher does not support empty row groups")
      val endOffset = rowsOffset + expectedRows.length
      val actualRows = rows.slice(rowsOffset, endOffset)
      rowsOffset = endOffset
      val sortedExpected = sort(expectedRows)
      val sortedRows = sort(actualRows)
      matchSorted(sortedExpected, sortedRows, diffString) match {
        case RowsMatch =>
        case RowsDontMatch(_) =>
          success = false
      }
      i += 1
    }

    if (rowsOffset < rows.length) {
      val endOffset = rows.length
      val actualRows = rows.slice(rowsOffset, endOffset)
      val sortedRows = sort(actualRows)
      matchSorted(IndexedSeq.empty, sortedRows, diffString) match {
        case RowsMatch =>
        case RowsDontMatch(_) =>
          success = false
      }
    }

    if (success) {
      RowsMatch
    } else {
      RowsDontMatch(diffString.result)
    }
  }

  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String =
    Rows.pretty(rows.map(row => VirtualValues.list(row: _*)).sorted(Rows.ANY_VALUE_ORDERING).map(l => l.asArray()))
}

case class OneToOneSortedPathsMatcher(
  pathVar: String,
  expected: IndexedSeq[Array[AnyValue]],
  listInAnyOrder: Boolean = false
) extends EqualRowsMatcher(listInAnyOrder) {

  override def toString: String =
    formatRows(expected) + s" in order sorted on paths $pathVar"

  override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): RowMatchResult = {

    assert(columns.contains(pathVar))

    // First check match in any order
    val sortedExpected = expected.map(row => VirtualValues.list(row: _*)).sorted(Rows.ANY_VALUE_ORDERING)
    val sortedRows = rows.map(row => VirtualValues.list(row: _*)).sorted(Rows.ANY_VALUE_ORDERING)

    matchSorted(sortedExpected, sortedRows) match {
      case bad: RowsDontMatch => bad
      case RowsMatch          => assertPathsInAscendingLengthPerPair(columns, rows)
    }
  }

  def assertPathsInAscendingLengthPerPair(
    columns: IndexedSeq[String],
    rows: IndexedSeq[Array[AnyValue]]
  ): RowMatchResult = {

    val errorStrings = new mutable.StringBuilder()

    val pathIndex = columns.indexOf(pathVar)

    val longestSeenPathPerSourceTargetPair = new mutable.HashMap[(Long, Long), (Int, VirtualPathValue)]()

    for (row <- rows) {
      val path = row(pathIndex).asInstanceOf[VirtualPathValue]
      val sourceNode = path.nodeIds().head
      val targetNode = path.nodeIds().last
      val pair = (sourceNode, targetNode)
      val pathLength = path.size()

      longestSeenPathPerSourceTargetPair.get(pair) match {
        case None => longestSeenPathPerSourceTargetPair.put(pair, pathLength -> path)
        case Some(previousLength -> _) if previousLength <= pathLength =>
          longestSeenPathPerSourceTargetPair.put(pair, pathLength -> path)
        case Some(previousLength -> previousPath) if previousLength > pathLength =>
          errorStrings.append(
            s"Path $previousPath came before path $path even though it's longer!\n"
          )
      }
    }
    if (errorStrings.isEmpty) {
      RowsMatch
    } else {
      RowsDontMatch(errorStrings.result)
    }
  }

  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

case class EqualInOrder(expected: IndexedSeq[Array[AnyValue]], listInAnyOrder: Boolean = false)
    extends EqualRowsMatcher(listInAnyOrder) {
  override def toString: String = formatRows(expected) + " in order"

  private def sort = sortIfNeeded(sortRows = false)(_)

  override def matches(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): RowMatchResult = {
    matchSorted(sort(expected), sort(rows))
  }
  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

case class RowCount(expected: Int) extends RowsMatcher {
  override def toString: String = s"Expected $expected"

  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean =
    rows.size == expected

  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

case class DisallowValues(columnValuePredicates: Seq[(String, AnyValue => Boolean)]) extends RowsMatcher {
  override def toString: String = s"Expected no rows to contain disallowed value"

  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {
    val columnOffsets = columnValuePredicates.map { case (c, _) => columns.indexOf(c) }
    val columnPredicates = columnValuePredicates.map { case (_, p) => p }

    var i = 0
    while (i < rows.size) {
      val row = rows(i)
      var c = 0
      while (c < columnOffsets.size) {
        val value = row(columnOffsets(c))
        val columnPredicate = columnPredicates(c)
        if (!columnPredicate(value)) {
          return false
        }
        c += 1
      }
      i += 1
    }
    true
  }

  override protected def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

case class CustomRowsMatcher(inner: Matcher[Seq[Array[AnyValue]]]) extends RowsMatcher {
  override def toString: String = s"Rows matching $inner"

  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean =
    inner(rows).matches
  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
}

trait RowOrderMatcher extends RowsMatcher {
  protected var inner: Option[RowOrderMatcher] = None

  @tailrec
  private def append(x: RowOrderMatcher): Unit =
    inner match {
      case None       => inner = Some(x)
      case Some(tail) => tail.append(x)
    }

  def asc(column: String): RowOrderMatcher = {
    append(new Ascending(column))
    this
  }

  def desc(column: String): RowOrderMatcher = {
    append(new Descending(column))
    this
  }

  def groupBy(columns: String*): RowOrderMatcher = {
    append(new GroupBy(None, None, columns: _*))
    this
  }

  def groupBy(nGroups: Int, groupSize: Int, columns: String*): RowOrderMatcher = {
    append(new GroupBy(Some(nGroups), Some(groupSize), columns: _*))
    this
  }

  def description: String

  override def toString: String = {
    val descriptions = new ArrayBuffer[String]
    foreach(x => descriptions += x.description)
    descriptions.mkString("Rows ", ", ", "")
  }

  override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {
    foreach(_.reset())
    if (rows.isEmpty) {
      return false
    }

    for (row <- rows) {
      if (!onRow(columns, row)) {
        return false
      }
    }
    onComplete()
  }

  override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)

  def foreach(f: RowOrderMatcher => Unit): Unit = {
    var x: Option[RowOrderMatcher] = Some(this)
    while (x.nonEmpty) {
      val curr = x.get
      f(curr)
      x = curr.inner
    }
  }

  def reset(): Unit
  def onRow(columns: IndexedSeq[String], row: Array[AnyValue]): Boolean
  def onComplete(): Boolean
}

class GroupBy(val nGroups: Option[Int], val groupSize: Option[Int], val groupingColumns: String*)
    extends RowOrderMatcher {

  def description: String = {
    val n = nGroups.map(n => s"$n ").getOrElse("")
    val size = groupSize.map(n => s"of size $n ").getOrElse("")
    s"in ${n}groups ${size}by '${groupingColumns.mkString(",")}'"
  }

  private var groupCount: Int = 0
  private var currentCount: Int = 0
  private var previous: Seq[AnyValue] = _
  private val seenGroupingKeys = mutable.Set[Seq[AnyValue]]()

  override def reset(): Unit = {
    seenGroupingKeys.clear()
    previous = null
  }

  override def onRow(columns: IndexedSeq[String], row: Array[AnyValue]): Boolean = {

    val is = groupingColumns.map(col => {
      val i = columns.indexOf(col)
      if (i == -1)
        throw new IllegalArgumentException(
          s"group by column '$col' is not part of result columns '${columns.mkString(",")}'"
        )
      i
    })

    val current = is.map(row)
    if (current == previous) {
      currentCount += 1
      inner.forall(_.onRow(columns, row))
    } else {
      if (!seenGroupingKeys.add(current)) false
      else {
        if (currentCount > 0 && groupSize.exists(_ != currentCount)) {
          return false
        }
        groupCount += 1
        currentCount = 1
        previous = current
        inner.forall { tail =>
          tail.reset()
          tail.onRow(columns, row)
        }
      }
    }
  }

  override def onComplete(): Boolean = {
    groupSize.forall(_ == currentCount) && nGroups.forall(_ == groupCount)
  }
}

class Ascending(val column: String) extends Sort {
  def description: String = s"sorted asc by '$column'"

  override def initialValue: AnyValue = VirtualValues.EMPTY_MAP

  override protected def wantedOrder(cmp: Int): Boolean = cmp <= 0
}

class Descending(val column: String) extends Sort {
  def description: String = s"sorted desc by '$column'"

  override def initialValue: AnyValue = Values.NO_VALUE

  override protected def wantedOrder(cmp: Int): Boolean = cmp >= 0
}

abstract class Sort extends RowOrderMatcher {

  private var previous: AnyValue = _

  override def reset(): Unit = previous = initialValue

  override def onRow(columns: IndexedSeq[String], row: Array[AnyValue]): Boolean = {
    val i = columns.indexOf(column)
    if (i == -1)
      throw new IllegalArgumentException(
        s"sort column '$column' is not part of result columns '${columns.mkString(",")}'"
      )

    val current = row(i)
    val cmp = AnyValues.COMPARATOR.compare(previous, current)
    if (cmp == 0) {
      inner.forall(_.onRow(columns, row))
    } else if (wantedOrder(cmp)) {
      previous = current
      inner.forall { tail =>
        tail.foreach(_.reset())
        tail.onRow(columns, row)
      }
    } else {
      false
    }
  }

  override def onComplete(): Boolean = true

  def column: String
  def initialValue: AnyValue
  protected def wantedOrder(cmp: Int): Boolean
}

object RowDiffStringBuilder {
  val PARTIALLY_ORDERED_GROUP_SEPARATOR = "    --- <group boundary> ---\n"
}

class RowDiffStringBuilder {
  private val sb = new mutable.StringBuilder()

  def onEndGroup(): Unit = {
    sb ++= PARTIALLY_ORDERED_GROUP_SEPARATOR
  }

  def onMatchStreak(size: Int): Unit = {
    sb ++= s"    ... $size matching rows ...\n"
  }

  def onMissingRow(row: ListValue): Unit = {
    sb ++= " - " + Rows.prettyRow(row.asArray())
  }

  def onExtraRow(row: ListValue): Unit = {
    sb ++= " + " + Rows.prettyRow(row.asArray())
  }

  def result: String = sb.result()
}

object SortListValueMapper extends ValueMapper[AnyValue] {
  override def mapPath(value: VirtualPathValue): AnyValue = value
  override def mapNode(value: VirtualNodeValue): AnyValue = value
  override def mapRelationship(value: VirtualRelationshipValue): AnyValue = value

  override def mapMap(value: MapValue): AnyValue = {
    val newMap = new MapValueBuilder()
    value.foreach((key, value) => newMap.add(key, value.map(this)))
    newMap.build()
  }
  override def mapNoValue(): AnyValue = Values.NO_VALUE

  override def mapSequence(seq: SequenceValue): AnyValue = {
    val array = new Array[AnyValue](seq.intSize())
    for (i <- 0 until seq.intSize()) {
      array(i) = seq.value(i).map(this)
    }
    java.util.Arrays.sort(array, AnyValues.COMPARATOR)
    VirtualValues.list(array: _*)
  }
  override def mapText(value: TextValue): AnyValue = value
  override def mapBoolean(value: BooleanValue): AnyValue = value
  override def mapNumber(value: NumberValue): AnyValue = value
  override def mapDateTime(value: DateTimeValue): AnyValue = value
  override def mapLocalDateTime(value: LocalDateTimeValue): AnyValue = value
  override def mapDate(value: DateValue): AnyValue = value
  override def mapTime(value: TimeValue): AnyValue = value
  override def mapLocalTime(value: LocalTimeValue): AnyValue = value
  override def mapDuration(value: DurationValue): AnyValue = value
  override def mapPoint(value: PointValue): AnyValue = value
}
