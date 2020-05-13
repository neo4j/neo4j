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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.plandescription.Arguments.ByteCode
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHitRatio
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeImpl
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.SourceCode
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.Arguments.Version

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object renderAsTreeTable extends (InternalPlanDescription => String) {
  val UNNAMED_PATTERN = """  (REL|NODE|UNNAMED|FRESHID|AGGREGATION)(\d+)"""
  val UNNAMED_PARAMS_PATTERN = """  (AUTOINT|AUTODOUBLE|AUTOSTRING|AUTOLIST)(\d+)"""
  val OPERATOR = "Operator"
  val DETAILS = "Details"
  private val ESTIMATED_ROWS = "Estimated Rows"
  private val ROWS = "Rows"
  private val HITS = "DB Hits"
  private val MEMORY = "Memory (Bytes)"
  private val PAGE_CACHE_HITS = "Page Cache Hits"
  private val PAGE_CACHE_MISSES = "Page Cache Misses"
  private val PAGE_CACHE_HIT_RATIO = "Page Cache Hit Ratio"
  private val TIME = "Time (ms)"
  private val ORDER = "Order"
  val MAX_DETAILS_COLUMN_WIDTH = 100
  private val OTHER = "Other"
  private val HEADERS = Seq(OPERATOR, DETAILS, ESTIMATED_ROWS, ROWS, HITS, MEMORY, PAGE_CACHE_HITS, PAGE_CACHE_MISSES, PAGE_CACHE_HIT_RATIO, TIME,
    ORDER, OTHER)
  private val newLine = System.lineSeparator()
  private val SEPARATOR = ","

  def apply(plan: InternalPlanDescription): String = {

    val columns = new mutable.HashMap[String, Int]()
    val lines = accumulate(plan, columns)
    val headers = HEADERS.filter(columns.contains)

    def width(header:String) = {
      2 + math.max(header.length, columns(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (lines.size * 2 + 3))

    def pad(width:Int, char:String=" ") = for (_ <- 1 to width) result.append(char)

    def writeConnection(line: LineDetails) = {
      val connection = line.connection.get
      result.append(connection)
      pad(width(OPERATOR) - connection.length - 1)
    }

    def divider(line:LineDetails = null) = {
      for (header <- headers) {
        if (line != null && header == OPERATOR && line.connection.isDefined) {
          result.append("| ")
          writeConnection(line)
        } else {
          result.append("+")
          pad(width(header), "-")
        }
      }
      result.append("+").append(newLine)
    }

    for (line <- Line(OPERATOR, headers.map(header => header -> Left(header)).toMap) +: lines) {
      if (line.newRow) {
        divider(line)
      }
      for (header <- headers) {
        val detail = line(header)
        result.append("| ")
        if (header == OPERATOR && line.connection.isDefined && !line.newRow) {
          writeConnection(line)
        } else {
          detail match {
            case Left(text) =>
              result.append(text)
              pad(width(header) - text.length - 2)
            case Right(text) =>
              pad(width(header) - text.length - 2)
              result.append(text)
          }
          result.append(" ")
        }
      }
      result.append("|").append(newLine)
    }
    divider()

    result.toString()
  }

  private def accumulate(incoming: InternalPlanDescription, columns: mutable.Map[String, Int]): Seq[Line] = {

    val stack = new mutable.ArrayStack[(InternalPlanDescription, Level)]
    stack.push((compactPlan(incoming), Root))
    val lines = new ArrayBuffer[Line]()
    while (stack.nonEmpty) {
      val (plan, level) = stack.pop()

      val line = level.line + plan.name
      mapping(OPERATOR, Left(line), columns)
      val rowLines = tableRowLines(tableRow(plan, columns))
      rowLines.headOption.foreach(row => lines.append(Line(line, row, level.connector)))
      val childConnector = plan.children match {
        case NoChildren =>
          if (stack.isEmpty) None else level.connector
        case SingleChild(inner) =>
          stack.push((compactPlan(inner), level.child))
          level.child.connector
        case TwoChildren(lhs, rhs) =>
          stack.push((compactPlan(lhs), level.child))
          stack.push((compactPlan(rhs), level.fork))
          level.fork.connector
      }
      rowLines.tail.foreach(row => lines.append(Line("", row, childConnector.map(_.replace("\\", "")), false)))
    }
    lines
  }

  private def tableRowLines(operatorRows: Map[String, Seq[Justified]]): Seq[Map[String, Justified]] = {
    val numRows = operatorRows
      .map { case (_, rows) => rows.length }
      .max
    (0 until numRows)
      .map(row => operatorRows.mapValues(_.lift(row).getOrElse(Left(""))))
  }

  private def compactPlan(plan: InternalPlanDescription): InternalPlanDescription = {
    @tailrec
    def compactPlanAcc(acc: Seq[InternalPlanDescription], plan: InternalPlanDescription):
    Seq[InternalPlanDescription] = {
      plan.children match {
        case SingleChild(inner) if !plan.arguments.exists(_.isInstanceOf[Details]) &&
                                   !inner.arguments.exists(_.isInstanceOf[Details]) &&
                                   otherFields(plan).isEmpty &&
                                   otherFields(inner).isEmpty &&
                                   inner.name == plan.name => compactPlanAcc(acc :+ plan, inner)
        case _ => acc :+ plan
      }
    }
    val similar = compactPlanAcc(Seq.empty[InternalPlanDescription], plan)
    CompactedPlanDescription.create(similar)
  }

  private def tableRow(description: InternalPlanDescription, columns: mutable.Map[String,Int]): Predef.Map[String, Seq[Justified]] = description.arguments.flatMap {
    case EstimatedRows(count) => mapping(ESTIMATED_ROWS, Right(format(count)), columns)
    case Rows(count) => mapping(ROWS, Right(count.toString), columns)
    case DbHits(count) => mapping(HITS, Right(count.toString), columns)
    case Memory(count) => mapping(MEMORY, Right(count.toString), columns)
    case PageCacheHits(count) => mapping(PAGE_CACHE_HITS, Right(count.toString), columns)
    case PageCacheMisses(count) => mapping(PAGE_CACHE_MISSES, Right(count.toString), columns)
    case PageCacheHitRatio(ratio) => mapping(PAGE_CACHE_HIT_RATIO, Right("%.4f".format(ratio)), columns)
    case Time(nanos) => mapping(TIME, Right("%.3f".format(nanos/1000000.0)), columns)
    case Order(providedOrder) => mapping(ORDER, Left(providedOrder.prettifiedString), columns)
    case Details(detailsList) => {
      val detailsLines = splitDetails(detailsList.map(_.prettifiedString).toList, MAX_DETAILS_COLUMN_WIDTH)
      mapping(DETAILS, detailsLines.map(Left(_)), columns)
    }
    case _ => None
  }.toMap + (OTHER -> Seq(Left(other(description, columns))))

  protected[plandescription] def splitDetails(details: List[String], length: Int = MAX_DETAILS_COLUMN_WIDTH): Seq[String] = {
    var currentRow = ""
    var rows = Seq.empty[String]

    if (details.isEmpty) return Seq.empty

    details.init
      .foreach { detail =>
        val (newCurrentRow, newRows) = splitDetail(detail, currentRow, length, isLastDetail = false)
        currentRow = newCurrentRow
        rows = rows ++ newRows
      }

    val (newCurrentRow, newRows) = splitDetail(details.last, currentRow, length, isLastDetail = true)
    if (newCurrentRow.isEmpty) {
      rows ++ newRows
    } else {
      (rows ++ newRows) :+ newCurrentRow
    }
  }

  private def splitDetail(detail: String, currentRow: String, length: Int, isLastDetail: Boolean): (String, Seq[String]) = {
    val separator = if (isLastDetail) "" else SEPARATOR
    val spaceLeftOnCurrentRow = length - currentRow.length

    val (newCurrentRow, rows) =
      if (detail.length + separator.length <= spaceLeftOnCurrentRow) {
        // Can fit in the current row
        (currentRow + detail + separator, Seq.empty)
      } else if (detail.length + separator.length <= length) {
        // Can't fit in the current row, but it can fit on the next row - add detail to new row
        (detail + separator, Seq(currentRow))
      } else {
        // Too long to fit on it's own row - add to current row and continue on next row(s)
        val firstRow = currentRow + detail.take(spaceLeftOnCurrentRow)
        val multiRows = detail.drop(spaceLeftOnCurrentRow).grouped(length).toSeq
        val rows = firstRow +: multiRows
        if (rows.last.length + separator.length <= length) {
          (rows.last + separator, rows.take(rows.length - 1))
        } else {
          (separator, rows)
        }
      }

    // Add space only if it's not the last row and it can fit on the current row.
    if (!isLastDetail && newCurrentRow.length < length && newCurrentRow.nonEmpty) {
      (s"$newCurrentRow ", rows)
    } else {
      (newCurrentRow, rows)
    }
  }

  private def mapping(key: String, value: Seq[Justified], columns: mutable.Map[String,Int]) = {
    update(columns, key, value.map(_.length).max)
    Some(key -> value)
  }

  private def mapping(key: String, value: Justified, columns: mutable.Map[String,Int]) = {
    update(columns, key, value.length)
    Some(key -> Seq(value))
  }

  private def update(columns: mutable.Map[String, Int], key: String, length: Int): Unit = {
    columns.put(key, math.max(columns.getOrElse(key, 0), length))
  }

  private def otherFields(description: InternalPlanDescription) = {
    description.arguments.collect { case x
      if !x.isInstanceOf[Rows] &&
        !x.isInstanceOf[DbHits] &&
        !x.isInstanceOf[Memory] &&
        !x.isInstanceOf[GlobalMemory] &&
        !x.isInstanceOf[PageCacheHits] &&
        !x.isInstanceOf[PageCacheMisses] &&
        !x.isInstanceOf[PageCacheHitRatio] &&
        !x.isInstanceOf[EstimatedRows] &&
        !x.isInstanceOf[Order] &&
        !x.isInstanceOf[Planner] &&
        !x.isInstanceOf[PlannerImpl] &&
        !x.isInstanceOf[PlannerVersion] &&
        !x.isInstanceOf[Runtime] &&
        !x.isInstanceOf[RuntimeVersion] &&
        !x.isInstanceOf[SourceCode] &&
        !x.isInstanceOf[ByteCode] &&
        !x.isInstanceOf[Time] &&
        !x.isInstanceOf[RuntimeImpl] &&
        !x.isInstanceOf[Version] &&
        !x.isInstanceOf[Details] => PlanDescriptionArgumentSerializer.serialize(x)
    }
  }

  private def other(description: InternalPlanDescription, columns: mutable.Map[String,Int]): String = {
    val result = otherFields(description).mkString("; ").replaceAll(UNNAMED_PATTERN, "")
    if (result.nonEmpty) {
      update(columns, OTHER, result.length)
    }
    result
  }

  private def format(v: Double) = if (v.isNaN) v.toString else math.round(v).toString
}

trait LineDetails extends (String => Justified) {
  def connection: Option[String]
}

case class Line(tree: String, allColumns: Map[String, Justified], connection: Option[String] = None, newRow: Boolean = true) extends LineDetails {
  def apply(key: String): Justified = if (key == renderAsTreeTable.OPERATOR) {
    Left(tree)
  } else {
    allColumns.getOrElse(key, Left(""))
  }
}

sealed abstract class Justified(text:String) {
  def length: Int = text.length
}
case class Left(text:String) extends Justified(text)
case class Right(text:String) extends Justified(text)

sealed abstract class Level {
  def child: Level
  def fork: Level
  def line: String
  def connector: Option[String]
}
case object Root extends Level {
  override def child: Level = Child(1)
  override def fork: Level = Fork(2)
  override def line: String = "+"
  override def connector: Option[String] = None
}
case class Child(level:Int) extends Level {
  override def child: Level = Child(level)
  override def fork: Level = Fork(level+1)
  override def line: String = "| " * (level-1) + "+"
  override def connector: Option[String] = Some("| " * level)
}
case class Fork(level:Int) extends Level {
  override def child: Level = Child(level)
  override def fork: Level = Fork(level+1)
  override def line: String = "| " * (level-1) + "+"
  override def connector: Option[String] = Some("| " * (level-2) + "|\\")
}
