/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
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

object renderAsTreeTable {
  val UNNAMED_PATTERN = """  (REL|NODE|UNNAMED|FRESHID|AGGREGATION)(\d+)"""
  val UNNAMED_PARAMS_PATTERN = """  (AUTOINT|AUTODOUBLE|AUTOSTRING|AUTOLIST)(\d+)"""
  val OPERATOR = "Operator"
  val DETAILS = "Details"
  private val ESTIMATED_ROWS = "Estimated Rows"
  private val ROWS = "Rows"
  private val HITS = "DB Hits"
  private val MEMORY = "Memory (Bytes)"
  private val PAGE_CACHE = "Page Cache Hits/Misses"
  private val TIME = "Time (ms)"
  private val ORDER = "Ordered by"
  val MAX_DETAILS_COLUMN_WIDTH = 100
  private val OTHER = "Other"
  private val HEADERS = Seq(OPERATOR, DETAILS, ESTIMATED_ROWS, ROWS, HITS, MEMORY, PAGE_CACHE, TIME,
    ORDER, OTHER)
  private val newLine = System.lineSeparator()
  private val SEPARATOR = ","
  private val MERGABLE_COLUMNS = Set(PAGE_CACHE, TIME)
  private val MERGE_COLUMN_PADDING = ' '

  def apply(plan: InternalPlanDescription, withRawCardinalities: Boolean = false): String = {

    val rows = accumulate(plan, withRawCardinalities)
    val lengthByColumnName = columnLengths(rows)
    val headers = HEADERS.filter(lengthByColumnName.contains)

    def width(header: String) = {
      2 + math.max(header.length, lengthByColumnName(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (rows.size * 2 + 3))

    def pad(width:Int, char: Char = ' '): Unit = for (_ <- 1 to width) result.append(char)

    def divider(line: TableRow = null) = {
      for (header <- headers) {
        if (line != null && header == OPERATOR && line.connection.isDefined) {
          result.append("| ")
          val connection = line.connection.get
          result.append(connection)
          pad(width(OPERATOR) - connection.length - 1)
        } else if (line != null && line(header).isMergedColumn) {
          val columnSeparator = if (result.lastOption.contains(MERGE_COLUMN_PADDING)) '|' else '+'
          result.append(columnSeparator)
          pad(width(header), MERGE_COLUMN_PADDING)
        } else {
          result.append('+')
          pad(width(header), '-')
        }
      }
      result.append("+").append(newLine)
    }

    val headersRow = TableRow(OPERATOR, headers.map(header => header -> LeftJustifiedCell(false, header)).toMap, None, None, Set.empty)
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

  case class LevelledPlan(plan: InternalPlanDescription, level: Level)

  private def accumulate(incoming: InternalPlanDescription, withRawCardinalities: Boolean): Seq[TableRow] = {
    var lastSeenPipelineInfo: Option[PipelineInfo] = None
    val plansWithoutPipelineInfo = mutable.ArrayBuffer.empty[(LevelledPlan, Option[String])]
    var previousLevelledPlan: Option[LevelledPlan] = None
    val stack = new mutable.ArrayStack[(InternalPlanDescription, Level)]
    stack.push((compactPlan(incoming), Root))
    val rows = new ArrayBuffer[TableRow]()

    def appendRow(levelledPlan: LevelledPlan, childConnector: Option[String], mergeColumns: Boolean): Unit = {
      val planTreeValue = levelledPlan.level.line + levelledPlan.plan.name
      val mergedColumns = if (mergeColumns) MERGABLE_COLUMNS else Set.empty[String]
      val cells = tableRow(levelledPlan.plan, mergedColumns, withRawCardinalities)

      val childConnection = childConnector.map(_.replace("\\", ""))
      val row = TableRow(planTreeValue, cells, levelledPlan.level.connector, childConnection, mergedColumns)

      rows.append(row)
    }

    def flushPlansWithoutPipelineInfo(mergeColumns: Boolean): Unit = {
      plansWithoutPipelineInfo.foreach { case (lp, cc) => appendRow(lp, cc, mergeColumns) }
      plansWithoutPipelineInfo.clear()
    }

    while (stack.nonEmpty) {
      val (plan, level) = stack.pop()
      val levelledPlan = LevelledPlan(plan, level)

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

      pipelineInfo(levelledPlan.plan) match {
        case Some(currentInfo) =>
          val isMerged = isPlanMerged(currentInfo, lastSeenPipelineInfo)

          // Append rows for intermediate plans without info, if we're still part of the same fused pipeline we merge aggregated columns
          flushPlansWithoutPipelineInfo(isMerged)

          appendRow(levelledPlan, childConnector, isMerged)
          lastSeenPipelineInfo = Some(currentInfo)
        case None =>
          if (lastSeenPipelineInfo.isDefined) {
            // encountered a plan that does not have a pipeline info, but we have seen a pipeline info before
            // buffer this row (rather than immediately appending) until we know what pipeline it is in
            plansWithoutPipelineInfo += ((levelledPlan, childConnector))
          } else {
            assert(plansWithoutPipelineInfo.isEmpty, "In this case, every operator in the plan should have no pipeline info")
            appendRow(levelledPlan, childConnector, mergeColumns = false)
          }
      }

      previousLevelledPlan = Some(levelledPlan)
    }

    flushPlansWithoutPipelineInfo(mergeColumns = false)

    rows
  }

  private def pipelineInfo(plan: InternalPlanDescription): Option[PipelineInfo] = {
    plan.arguments.collectFirst { case info: PipelineInfo => info }
  }

  private def isPlanMerged(planInfo: PipelineInfo, maybeOtherPlanInfo: Option[PipelineInfo]): Boolean = {
    maybeOtherPlanInfo match {
      case Some(otherPlanInfo) => planInfo.pipelineId == otherPlanInfo.pipelineId && planInfo.fused && otherPlanInfo.fused
      case None => false
    }
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

  private def tableRow(description: InternalPlanDescription, mergedColumns: Set[String], withRawCardinalities: Boolean): Map[String, Cell] = {
    def leftJustifiedMapping(key: String, lines: String*): (String, Cell) =
      key -> LeftJustifiedCell(mergedColumns.contains(key), lines:_*)
    def rightJustifiedMapping(key: String, lines: String*): (String, Cell) =
      key -> RightJustifiedCell(mergedColumns.contains(key), lines:_*)

    val argumentColumns = description.arguments.collect {
      case EstimatedRows(effectiveCardinality, cardinality) => rightJustifiedMapping(ESTIMATED_ROWS, format(effectiveCardinality, cardinality, withRawCardinalities))
      case Rows(count) => rightJustifiedMapping(ROWS, count.toString)
      case DbHits(count) => rightJustifiedMapping(HITS, count.toString)
      case Memory(count) => rightJustifiedMapping(MEMORY, count.toString)
      case PageCacheHits(hits) =>
        val misses = description.arguments.collectFirst { case PageCacheMisses(missCount) => missCount }
        rightJustifiedMapping(PAGE_CACHE, s"$hits/${misses.getOrElse(0)}")
      case Time(nanos) => rightJustifiedMapping(TIME, "%.3f".format(nanos/1000000.0))
      case Order(providedOrder) => leftJustifiedMapping(ORDER, providedOrder.prettifiedString)
      case Details(detailsList) =>
        val detailsLines = splitDetails(detailsList.map(_.prettifiedString).toList, MAX_DETAILS_COLUMN_WIDTH)
        leftJustifiedMapping(DETAILS, detailsLines:_*)
    }

    val otherColumn = OTHER -> LeftJustifiedCell(mergedColumns.contains(OTHER), other(description))

    (argumentColumns :+ otherColumn).toMap
  }

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

  private def otherFields(description: InternalPlanDescription) = {
    description.arguments.collect { case x
      if !x.isInstanceOf[Rows] &&
        !x.isInstanceOf[DbHits] &&
        !x.isInstanceOf[Memory] &&
        !x.isInstanceOf[GlobalMemory] &&
        !x.isInstanceOf[PageCacheHits] &&
        !x.isInstanceOf[PageCacheMisses] &&
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

  private def other(description: InternalPlanDescription): String = {
    otherFields(description).mkString("; ").replaceAll(UNNAMED_PATTERN, "")
  }

  private def format(effectiveCardinality: Double, cardinality: Option[Double], withRawCardinalities: Boolean) =
    if (withRawCardinalities) {
      s"${cardinality.getOrElse("Unknown")} ($effectiveCardinality)"
    } else if (effectiveCardinality.isNaN) {
      effectiveCardinality.toString
    } else {
      math.round(effectiveCardinality).toString
    }

  private def columnLengths(rows: Seq[TableRow]): Map[String, Int] = {
    rows
      .flatMap { row =>
        val operatorCell = OPERATOR -> LeftJustifiedCell(false, row.tree).length
        val otherCells = row.allColumns.toSeq.map { case (columnName, cell) => columnName -> cell.length }
        otherCells :+ operatorCell
      }
      .filter { case (_, length) => length > 0 }
      .foldLeft(mutable.Map.empty[String, Int]) { case (acc, (columnName, length)) =>
        val currentMax = acc.getOrElseUpdate(columnName, length)
        if (length > currentMax) {
          acc.update(columnName, length)
        }
        acc
      }
  }
}

/**
 * TableRow contains rendering information about a single entry in the plan table.
 *
 * @param tree - the plan tree string including the operator name
 * @param allColumns - arguments to be rendered
 * @param connection - tree connection to previous table entry
 * @param childConnection - tree connection on the next table entry
 */
case class TableRow(tree: String, allColumns: Map[String, Cell], connection: Option[String], childConnection: Option[String], mergedColumns: Set[String]) {
  def apply(key: String): Cell = {
    if (key == renderAsTreeTable.OPERATOR) {
      LeftJustifiedCell(false, tree)
    } else {
      val isMerged = mergedColumns.contains(key)
      allColumns.getOrElse(key, LeftJustifiedCell(isMerged, ""))
    }
  }

  def height: Int = allColumns.values.map(_.lines.length).max
}

sealed abstract class Cell {
  val lines: Seq[String]
  def length: Int = if (lines.nonEmpty) lines.maxBy(_.length).length else 0
  def isMergedColumn: Boolean
}

case class LeftJustifiedCell(isMergedColumn: Boolean, lines: String*) extends Cell
case class RightJustifiedCell(isMergedColumn: Boolean, lines: String*) extends Cell

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
