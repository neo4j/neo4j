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

    val (rows, columns) = accumulate(plan)
    val headers = HEADERS.filter(columns.contains)

    def width(header:String) = {
      2 + math.max(header.length, columns(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (rows.size * 2 + 3))

    def pad(width:Int, char:String=" "): Unit = for (_ <- 1 to width) result.append(char)

    def divider(line: TableRow = null) = {
      for (header <- headers) {
        if (line != null && header == OPERATOR && line.connection.isDefined) {
          result.append("| ")
          val connection = line.connection.get
          result.append(connection)
          pad(width(OPERATOR) - connection.length - 1)
        } else {
          result.append("+")
          pad(width(header), "-")
        }
      }
      result.append("+").append(newLine)
    }

    val headersRow = TableRow(OPERATOR, headers.map(header => header -> LeftJustifiedCell(header)).toMap, None, None)
    for (row <- headersRow +: rows) {
      divider(row)
      for (currentHeight <- 0 until row.height) {
        for (header <- headers) {
          val cell = row(header)
          val text = cell.lines.lift(currentHeight).getOrElse {
            if (header == OPERATOR)
              row.childConnection.getOrElse("")
            else
              ""
          }
          result.append("| ")
          cell match {
            case _: LeftJustifiedCell =>
              result.append(text)
              pad(width(header) - text.length - 2)
            case _: RightJustifiedCell =>
              pad(width(header) - text.length - 2)
              result.append(text)
          }
          result.append(" ")
        }
        result.append("|").append(newLine)
      }
    }
    divider()

    result.toString()
  }

  private def accumulate(incoming: InternalPlanDescription): (Seq[TableRow], Map[String, Int]) = {
    val columns = mutable.Map[String, Int]()
    val stack = new mutable.ArrayStack[(InternalPlanDescription, Level)]
    stack.push((compactPlan(incoming), Root))
    val rows = new ArrayBuffer[TableRow]()
    while (stack.nonEmpty) {
      val (plan, level) = stack.pop()

      val line = level.line + plan.name
      mapping(OPERATOR, LeftJustifiedCell(line), columns)
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
      rows.append(TableRow(line, tableRow(plan, columns), level.connector, childConnector.map(_.replace("\\", ""))))
    }
    (rows, columns.toMap)
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

  private def tableRow(description: InternalPlanDescription, columns: mutable.Map[String,Int]): Predef.Map[String, Cell] = description.arguments.flatMap {
    case EstimatedRows(count) => mapping(ESTIMATED_ROWS, RightJustifiedCell(format(count)), columns)
    case Rows(count) => mapping(ROWS, RightJustifiedCell(count.toString), columns)
    case DbHits(count) => mapping(HITS, RightJustifiedCell(count.toString), columns)
    case Memory(count) => mapping(MEMORY, RightJustifiedCell(count.toString), columns)
    case PageCacheHits(count) => mapping(PAGE_CACHE_HITS, RightJustifiedCell(count.toString), columns)
    case PageCacheMisses(count) => mapping(PAGE_CACHE_MISSES, RightJustifiedCell(count.toString), columns)
    case PageCacheHitRatio(ratio) => mapping(PAGE_CACHE_HIT_RATIO, RightJustifiedCell("%.4f".format(ratio)), columns)
    case Time(nanos) => mapping(TIME, RightJustifiedCell("%.3f".format(nanos/1000000.0)), columns)
    case Order(providedOrder) => mapping(ORDER, LeftJustifiedCell(providedOrder.prettifiedString), columns)
    case Details(detailsList) =>
      val detailsLines = splitDetails(detailsList.map(_.prettifiedString).toList, MAX_DETAILS_COLUMN_WIDTH)
      mapping(DETAILS, LeftJustifiedCell(detailsLines:_*), columns)
    case _ => None
  }.toMap + (OTHER -> LeftJustifiedCell(other(description, columns)))

  protected[plandescription] def splitDetails(details: List[String], length: Int = MAX_DETAILS_COLUMN_WIDTH): Seq[String] = {
    var currentLine = ""
    var lines = Seq.empty[String]

    if (details.isEmpty) return Seq.empty

    details.init
      .foreach { detail =>
        val (newCurrentLine, newLines) = splitDetail(detail, currentLine, length, isLastDetail = false)
        currentLine = newCurrentLine
        lines = lines ++ newLines
      }

    val (newCurrentLine, newLines) = splitDetail(details.last, currentLine, length, isLastDetail = true)
    lines ++= newLines
    if (newCurrentLine.strip().nonEmpty) {
      lines = lines :+ newCurrentLine
    }

    lines.map(_.strip())
  }

  private def splitDetail(detail: String, currentLine: String, length: Int, isLastDetail: Boolean): (String, Seq[String]) = {
    val separator = if (isLastDetail) "" else SEPARATOR
    val spaceLeftOnCurrentLine = length - currentLine.length

    val (newCurrentLine, lines) =
      if (detail.length + separator.length <= spaceLeftOnCurrentLine) {
        // Can fit in the current line
        (currentLine + detail + separator, Seq.empty)
      } else if (detail.length + separator.length <= length) {
        // Can't fit in the current line, but it can fit on the next line - add detail to new line
        (detail + separator, Seq(currentLine))
      } else {
        // Too long to fit on it's own line - add to current line and continue on next line(s)
        val firstLine = currentLine + detail.take(spaceLeftOnCurrentLine)
        val multiLines = detail.drop(spaceLeftOnCurrentLine).grouped(length).toSeq
        val lines = firstLine +: multiLines
        if (lines.last.length + separator.length <= length) {
          (lines.last + separator, lines.init)
        } else {
          (separator, lines)
        }
      }

    // If it is the last detail on the row, the space will be removed
    (s"$newCurrentLine ", lines)
  }

  private def mapping(key: String, value: Cell, columns: mutable.Map[String,Int]) = {
    update(columns, key, value.length)
    Some(key -> value)
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

/**
 * TableRow contains rendering information about a single entry in the plan table.
 *
 * @param tree - the plan tree string including the operator name
 * @param allColumns - arguments to be rendered
 * @param connection - tree connection to previous table entry
 * @param childConnection - tree connection on the next table entry
 */
case class TableRow(tree: String, allColumns: Map[String, Cell], connection: Option[String], childConnection: Option[String]) {
  def apply(key: String): Cell = if (key == renderAsTreeTable.OPERATOR) {
    LeftJustifiedCell(tree)
  } else {
    allColumns.getOrElse(key, LeftJustifiedCell(""))
  }

  def height: Int = allColumns.values.map(_.lines.length).max
}

sealed abstract class Cell {
  val lines: Seq[String]
  def length: Int = if (lines.nonEmpty) lines.maxBy(_.length).length else 0
}
case class LeftJustifiedCell(lines: String*) extends Cell
case class RightJustifiedCell(lines: String*) extends Cell

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
