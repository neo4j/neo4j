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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.Distinctness
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Memory
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.PlanDescriptionArgumentSerializer.serialize
import org.neo4j.cypher.internal.plandescription.renderAsTreeTable.splitDetails

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable

object renderAsTreeTable {
  private val MAX_DETAILS_COLUMN_WIDTH = 100
  private val newLine = System.lineSeparator()
  private val SEPARATOR = ","
  private val MERGE_COLUMN_PADDING = ' '

  def apply(
    plan: InternalPlanDescription,
    withRawCardinalities: Boolean = false,
    withDistinctness: Boolean = false
  ): String = {

    val table = TreeTableBuilder.buildTable(plan, withRawCardinalities, withDistinctness)
    val headers = Header.ALL.filter(table.columnLengths.contains)

    def width(header: String) = {
      2 + math.max(header.length, table.columnLengths(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (table.rows.size * 2 + 3))

    def pad(width: Int, char: Char = ' '): Unit = for (_ <- 1 to width) result.append(char)
    def columnSeparator(): Char = if (result.lastOption.contains(MERGE_COLUMN_PADDING)) '|' else '+'
    def divider(line: TableRow = null): Unit = {
      for (header <- headers) {
        if (line != null && header == Header.OPERATOR && line.childLevel.exists(_.connector.isDefined)) {
          result.append("| ")
          val connection = line.childLevel.get.connector.get
          result.append(connection)
          pad(width(Header.OPERATOR) - connection.length - 1)
        } else if (line != null && line(header).isMerged) {
          result.append(columnSeparator())
          pad(width(header), MERGE_COLUMN_PADDING)
        } else {
          result.append('+')
          pad(width(header), '-')
        }
      }
      result.append(columnSeparator()).append(newLine)
    }

    divider()
    for (row <- table.rowsWithHeader()) {
      for (currentHeight <- 0 until row.height) {
        for (header <- headers) {
          val cell = row(header)
          val text = cell.lines.lift(currentHeight).getOrElse {
            if (header == Header.OPERATOR)
              row.childLevel.flatMap(_.extension).getOrElse("")
            else
              ""
          }
          result.append("| ")
          if (cell.leftJustified) {
            result.append(text)
            pad(width(header) - text.length - 2)
          } else {
            pad(width(header) - text.length - 2)
            result.append(text)
          }
          result.append(" ")
        }
        result.append("|").append(newLine)
      }
      divider(row)
    }
    result.toString()
  }

  protected[plandescription] def splitDetails(
    details: List[String],
    maxLineLength: Int = MAX_DETAILS_COLUMN_WIDTH
  ): Seq[String] = {
    if (details.isEmpty) return Seq.empty

    val results = new StringBuilder()
    def lastLineLength: Int =
      results.lastIndexOf('\n') match {
        case -1 => results.length()
        case i  => results.length() - i - 1
      }

    def appendDetail(detail: String, isLast: Boolean): Unit = {
      val separatorLength = if (isLast) 0 else SEPARATOR.length
      val prefixSpace = if (lastLineLength == 0) "" else " "

      detail.replaceAll("\r", "").split("\n", -1).toSeq match {
        case Seq(singlePart) =>
          // A non-multiline detail
          def spaceLeftOnCurrentLine = maxLineLength - lastLineLength

          if (prefixSpace.length + singlePart.length + separatorLength <= spaceLeftOnCurrentLine) {
            // Can fit in the current line
            results.append(prefixSpace).append(singlePart)
          } else if (detail.length + separatorLength <= maxLineLength) {
            // Can't fit in the current line, but it can fit on the next line - add detail to new line
            results.append('\n').append(singlePart)
          } else if (prefixSpace.length < spaceLeftOnCurrentLine) {
            // Too long to fit on it's own line - add to current line and continue on next line(s)
            results.append(prefixSpace)
            val (head, tail) = detail.splitAt(spaceLeftOnCurrentLine)
            results.append(head)
            tail.grouped(maxLineLength).foreach { singlePartLine =>
              results.append('\n').append(singlePartLine)
            }
          } else {
            // Too long to fit on it's own line - add to next line(s)
            detail.grouped(maxLineLength).foreach { singlePartLine =>
              results.append('\n').append(singlePartLine)
            }
          }

          // Append separator
          if (!isLast) {
            if (lastLineLength + SEPARATOR.length <= maxLineLength) {
              results.append(SEPARATOR)
            } else {
              results.append('\n').append(SEPARATOR)
            }
          }
        case manyParts =>
          // A multiline detail
          manyParts.foreach { multiLineDetailLine =>
            if (multiLineDetailLine.length > maxLineLength) {
              multiLineDetailLine.grouped(maxLineLength).foreach { singlePartLine =>
                if (lastLineLength > 0) {
                  results.append('\n')
                }
                results.append(singlePartLine)
              }
            } else {
              if (lastLineLength > 0) {
                results.append('\n')
              }
              results.append(multiLineDetailLine)
            }
          }
          // Append separator
          if (!isLast) {
            if (lastLineLength + SEPARATOR.length <= maxLineLength) {
              results.append(SEPARATOR).append('\n')
            } else {
              results.append('\n').append(SEPARATOR)
            }
          }

      }
    }

    details.init.foreach(appendDetail(_, isLast = false))
    appendDetail(details.last, isLast = true)

    results.result().linesIterator.toSeq
  }
}

private object Header {
  val OPERATOR = "Operator"
  val ID = "Id"
  val DETAILS = "Details"
  val ESTIMATED_ROWS = "Estimated Rows"
  val ROWS = "Rows"
  val HITS = "DB Hits"
  val MEMORY = "Memory (Bytes)"
  val PAGE_CACHE = "Page Cache Hits/Misses"
  val TIME = "Time (ms)"
  val ORDER = "Ordered by"
  val DISTINCTNESS = "Distinctness"
  val PIPELINE = "Pipeline"

  val ALL: Seq[String] =
    Seq(OPERATOR, ID, DETAILS, ESTIMATED_ROWS, ROWS, HITS, MEMORY, PAGE_CACHE, TIME, ORDER, DISTINCTNESS, PIPELINE)
}

/**
 * Rendering information about all entries in the plan table.
 */
case class Table(rows: Seq[TableRow], columnLengths: Map[String, Int]) {
  def headers(): Seq[String] = columnLengths.keys.toSeq
  def rowsWithHeader(): Seq[TableRow] = headerRow() +: rows
  private def headerRow(): TableRow = TableRow(headers().map(h => h -> Cell.left(h)).toMap, None)
}

/**
 * TableRow contains rendering information about a single entry in the plan table.
 */
case class TableRow(columns: Map[String, Cell], childLevel: Option[Level]) {
  def apply(key: String): Cell = columns.getOrElse(key, Cell.left(""))
  def height: Int = columns.values.map(_.lines.length).reduceOption(math.max).getOrElse(1)
}

case class BuildingRow(levelledPlan: LevelledPlan, values: Map[String, Cell]) {
  def add(newValues: (String, Cell)*): BuildingRow = copy(values = values ++ newValues)
  def build(): TableRow = TableRow(values, levelledPlan.childLevel)
}

case class Cell(lines: Seq[String], leftJustified: Boolean, isMerged: Boolean) {
  def length: Int = if (lines.nonEmpty) lines.maxBy(_.length).length else 0
}

object Cell {
  def left(lines: String*): Cell = Cell(lines, leftJustified = true, isMerged = false)
  def right(lines: String*): Cell = Cell(lines, leftJustified = false, isMerged = false)
}

sealed abstract class Level {
  def child: Level
  def fork: Level
  def line: String
  def connector: Option[String]
  def extension: Option[String]
}

case object Root extends Level {
  override def child: Level = Child(1)
  override def fork: Level = Fork(2)
  override def line: String = ""
  override def connector: Option[String] = None
  override def extension: Option[String] = None
}

case class Child(level: Int) extends Level {
  override def child: Level = Child(level)
  override def fork: Level = Fork(level + 1)
  override def line: String = "| " * (level - 1)
  override def connector: Option[String] = Some("| " * level)
  override def extension: Option[String] = connector
}

case class Fork(level: Int) extends Level {
  override def child: Level = Child(level)
  override def fork: Level = Fork(level + 1)
  override def line: String = "| " * (level - 1)
  override def connector: Option[String] = Some("| " * (level - 2) + "|\\")
  override def extension: Option[String] = Some("| " * (level - 1))
}

case class LevelledPlan(
  plan: InternalPlanDescription,
  level: Level,
  childLevel: Option[Level],
  info: Option[PipelineInfo]
)

object LevelledPlan {

  def apply(plan: InternalPlanDescription, level: Level): LevelledPlan = {
    new LevelledPlan(plan, level, None, plan.arguments.collectFirst { case info: PipelineInfo => info })
  }
}

private object TreeTableBuilder {

  private val mergers = Seq[RowMerger](
    new MergeIfEqualFusedPipeline(Header.TIME, Header.PAGE_CACHE),
    new MergeAndEraseIfEqualPipeline(Header.PIPELINE),
    new MergeEqualValues(Header.ORDER)
  )

  def buildTable(
    rootPlan: InternalPlanDescription,
    withRawCardinalities: Boolean,
    withDistinctness: Boolean
  ): Table = {
    inferMissingPipelineInfo(compactAndCollectPlans(rootPlan))
      .foldLeft(new TreeTableBuilder(withRawCardinalities, withDistinctness)) { case (builder, plan) =>
        builder.add(plan)
      }
      .result()
  }

  private def compactAndCollectPlans(rootPlan: InternalPlanDescription): Iterator[LevelledPlan] =
    new Iterator[LevelledPlan] {
      private val stack = mutable.Stack[LevelledPlan](LevelledPlan(compactPlan(rootPlan), Root))

      override def hasNext: Boolean = stack.nonEmpty

      override def next(): LevelledPlan = {
        val levelledPlan = stack.pop()
        levelledPlan.plan.children match {
          case SingleChild(inner) =>
            stack.push(LevelledPlan(compactPlan(inner), levelledPlan.level.child))
          case TwoChildren(lhs, rhs) =>
            stack.push(LevelledPlan(compactPlan(lhs), levelledPlan.level.child))
            stack.push(LevelledPlan(compactPlan(rhs), levelledPlan.level.fork))
          case NoChildren =>
        }
        levelledPlan.copy(childLevel = stack.headOption.map(_.level))
      }
    }

  private def compactPlan(plan: InternalPlanDescription): InternalPlanDescription = {
    @tailrec
    def compactPlanAcc(
      acc: Seq[InternalPlanDescription],
      plan: InternalPlanDescription
    ): Seq[InternalPlanDescription] = {
      plan.children match {
        case SingleChild(inner)
          if !plan.arguments.exists(a => a.isInstanceOf[Details] || a.isInstanceOf[PipelineInfo]) &&
            !inner.arguments.exists(a => a.isInstanceOf[Details] || a.isInstanceOf[PipelineInfo]) &&
            inner.name == plan.name => compactPlanAcc(acc :+ plan, inner)
        case _ => acc :+ plan
      }
    }
    val similar = compactPlanAcc(Seq.empty[InternalPlanDescription], plan)
    CompactedPlanDescription.create(similar)
  }

  private def inferMissingPipelineInfo(input: Iterator[LevelledPlan]): Iterator[LevelledPlan] =
    new Iterator[LevelledPlan] {
      private val buffer = mutable.Queue.empty[LevelledPlan]
      private var lastSeenPipeline: Option[PipelineInfo] = None

      override def hasNext: Boolean = input.hasNext || buffer.nonEmpty

      override def next(): LevelledPlan = {
        if (buffer.isEmpty) fillBufferAndInferPipeline()
        buffer.dequeue()
      }

      private def fillBufferAndInferPipeline(): Unit = {
        while (input.hasNext && (buffer.isEmpty || buffer.last.info.isEmpty)) {
          buffer.enqueue(input.next())
        }
        if (buffer.size >= 2 && lastSeenPipeline.exists(_.fused) && lastSeenPipeline == buffer.last.info) {
          checkOnlyWhenAssertionsAreEnabled(buffer.forall(p => p.info.isEmpty || p.info == lastSeenPipeline))
          buffer.indices.foreach(_ => buffer.enqueue(buffer.dequeue().copy(info = lastSeenPipeline)))
        }
        lastSeenPipeline = buffer.last.info
      }
    }
}

private class TreeTableBuilder private (
  private val withRawCardinalities: Boolean,
  private val withDistinctness: Boolean
) {
  private val rows = mutable.Buffer.empty[TableRow]
  private var unmergedRow: Option[BuildingRow] = None
  private val lengths = mutable.Map.empty[String, Int]

  def add(plan: LevelledPlan): TreeTableBuilder = {
    val row = tableRow(plan)
    unmergedRow.foreach(unmerged => addRow(merge(unmerged, Some(row))))
    unmergedRow = Some(row)
    this
  }

  private def merge(row: BuildingRow, nextRow: Option[BuildingRow]): BuildingRow = {
    TreeTableBuilder.mergers.foldLeft(row) {
      case (mergedRow, merger) => nextRow.map(next => merger.merge(mergedRow, next)).getOrElse(mergedRow)
    }
  }

  private def addRow(row: BuildingRow): Unit = {
    row.values.foreach {
      case (key, value) =>
        val length = value.length
        if (length > 0 && lengths.get(key).forall(_ < length)) {
          lengths.update(key, length)
        }
    }
    rows += row.build()
  }

  def result(): Table = {
    unmergedRow.foreach(r => addRow(merge(r, None)))
    Table(rows.toSeq, lengths)
  }

  private def tableRow(levelledPlan: LevelledPlan): BuildingRow = {
    val plan = levelledPlan.plan

    val argumentColumns = plan.arguments.collect {
      case EstimatedRows(effectiveCardinality, cardinality) =>
        Header.ESTIMATED_ROWS -> Cell.right(format(effectiveCardinality, cardinality))
      case Rows(count)   => Header.ROWS -> Cell.right(count.toString)
      case DbHits(count) => Header.HITS -> Cell.right(count.toString)
      case Memory(count) => Header.MEMORY -> Cell.right(count.toString)
      case PageCacheHits(hits) =>
        val misses = plan.arguments.collectFirst { case PageCacheMisses(missCount) => missCount }
        Header.PAGE_CACHE -> Cell.right(s"$hits/${misses.getOrElse(0)}")
      case Time(nanos)          => Header.TIME -> Cell.right("%.3f".format(nanos / 1000000.0))
      case Order(providedOrder) => Header.ORDER -> Cell.left(providedOrder.prettifiedString)
      case Distinctness(distinctness) if withDistinctness =>
        Header.DISTINCTNESS -> Cell.left(distinctness.prettifiedString)
      case Details(detailsList) =>
        Header.DETAILS -> Cell.left(splitDetails(detailsList.map(_.prettifiedString).toList): _*)
      case pipeline: PipelineInfo => Header.PIPELINE -> Cell.left(serialize(pipeline).toString)
    }

    val idColumn = Header.ID -> Cell.right(plan.id.x.toString)

    val operatorColumn = Header.OPERATOR -> Cell.left(levelledPlan.level.line + "+" + levelledPlan.plan.name)

    BuildingRow(levelledPlan, (argumentColumns :+ operatorColumn :+ idColumn).toMap)
  }

  private def format(effectiveCardinality: Double, cardinality: Option[Double]): String =
    if (withRawCardinalities) {
      s"${cardinality.getOrElse("Unknown")} ($effectiveCardinality)"
    } else if (effectiveCardinality.isNaN) {
      effectiveCardinality.toString
    } else {
      math.round(effectiveCardinality).toString
    }
}

trait RowMerger {
  def merge(row: BuildingRow, next: BuildingRow): BuildingRow
}

abstract class PipelineRowMerger(keys: Seq[String]) extends RowMerger {

  final override def merge(row: BuildingRow, next: BuildingRow): BuildingRow =
    if (shouldMerge(row, next)) doMerge(row) else row

  def shouldMerge(row: BuildingRow, next: BuildingRow): Boolean =
    shouldMerge(row.levelledPlan.info, next.levelledPlan.info)

  def doMerge(row: BuildingRow): BuildingRow =
    row.add(keys.map(key => key -> doMerge(row.values.getOrElse(key, Cell.left("")))): _*)
  def doMerge(cell: Cell): Cell = cell.copy(isMerged = true)
  def shouldMerge(pipeline: Option[PipelineInfo], next: Option[PipelineInfo]): Boolean
}

class MergeIfEqualFusedPipeline(keys: String*) extends PipelineRowMerger(keys) {

  override def shouldMerge(pipeline: Option[PipelineInfo], next: Option[PipelineInfo]): Boolean =
    pipeline.exists(p => p.fused && next.contains(p))
}

class MergeAndEraseIfEqualPipeline(keys: String*) extends PipelineRowMerger(keys) {

  override def shouldMerge(pipeline: Option[PipelineInfo], next: Option[PipelineInfo]): Boolean =
    pipeline == next && pipeline.isDefined
  override def doMerge(cell: Cell): Cell = cell.copy(lines = Seq(""), isMerged = true)
}

class MergeEqualValues(key: String) extends RowMerger {

  override def merge(row: BuildingRow, next: BuildingRow): BuildingRow = {
    (row.values.get(key), next.values.get(key)) match {
      case (Some(cell), Some(nextCell)) if cell == nextCell && cell.lines.nonEmpty => doMerge(row, cell)
      case _                                                                       => row
    }
  }

  private def doMerge(row: BuildingRow, cell: Cell): BuildingRow =
    row.add(key -> cell.copy(lines = Seq(""), isMerged = true))
}
