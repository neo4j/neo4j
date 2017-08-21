/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments._

import scala.collection.{Map, mutable}

object renderAsTreeTable extends (InternalPlanDescription => String) {
  private val UNNAMED_PATTERN = """  (UNNAMED|FRESHID|AGGREGATION)\d+"""
  val OPERATOR = "Operator"
  private val ESTIMATED_ROWS = "Estimated Rows"
  private val ROWS = "Rows"
  private val HITS = "DB Hits"
  private val PAGE_CACHE_HITS = "Page Cache Hits"
  private val PAGE_CACHE_MISSES = "Page Cache Misses"
  private val PAGE_CACHE_HIT_RATIO = "Page Cache Hit Ratio"
  private val TIME = "Time (ms)"
  val VARIABLES = "Variables"
  val MAX_VARIABLE_COLUMN_WIDTH = 100
  private val OTHER = "Other"
  private val HEADERS = Seq(OPERATOR, ESTIMATED_ROWS, ROWS, HITS, PAGE_CACHE_HITS, PAGE_CACHE_MISSES, PAGE_CACHE_HIT_RATIO, TIME,
    VARIABLES, OTHER)
  private val newLine = System.lineSeparator()

  def apply(plan: InternalPlanDescription): String = {

    implicit val columns = new mutable.HashMap[String, Int]()
    val lines = accumulate(plan)

    def compactLine(line: Line, previous: Seq[(Line, CompactedLine)]) = {
      val repeatedVariables = if (previous.nonEmpty) previous.head._1.variables.intersect(line.variables) else Set.empty[String]
      CompactedLine(line, repeatedVariables)
    }

    val compactedLines = lines.reverse.foldLeft(Seq.empty[(Line, CompactedLine)]) { (acc, line) =>
      val compacted = compactLine(line, acc)
      if (compacted.formattedVariables.nonEmpty)
        update(columns, VARIABLES, compacted.formattedVariables.length)
      (line, compacted) +: acc
    } map (_._2)

    val headers = HEADERS.filter(columns.contains)

    def width(header:String) = {
      2 + math.max(header.length, columns(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (lines.size * 2 + 3))

    def pad(width:Int, char:String=" ") =
      for (_ <- 1 to width) result.append(char)
    def divider(line:LineDetails = null) = {
      for (header <- headers) {
        if (line != null && header == OPERATOR && line.connection.isDefined) {
          result.append("|")
          val connection = line.connection.get
          result.append(" ").append(connection)
          pad(width(header) - connection.length - 1)
        } else {
          result.append("+")
          pad(width(header), "-")
        }
      }
      result.append("+").append(newLine)
    }

    for ( line <- Seq(Line( OPERATOR, headers.map(header => header -> Left(header)).toMap, Set(VARIABLES))) ++
      compactedLines ) {
      divider(line)
      for ( header <- headers ) {
        val detail = line(header)
        result.append("| ")
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
      result.append("|").append(newLine)
    }
    divider()

    result.toString()
  }

  private def accumulate(incoming: InternalPlanDescription, level: Level = Root)(implicit columns: mutable.Map[String,
    Int]): Seq[Line] = {
    val plan = compactPlan(incoming)
    val line = level.line + plan.name
    mapping(OPERATOR, Left(line))
    Seq(Line(line, details(plan), plan.variables, level.connector)) ++ (plan.children match {
      case NoChildren => Seq.empty
      case SingleChild(inner) => accumulate(inner, level.child)
      case TwoChildren(lhs, rhs) => accumulate(rhs, level.fork) ++ accumulate(lhs, level.child)
    })
  }

  private def compactPlan(plan: InternalPlanDescription): InternalPlanDescription = {
    def compactPlanAcc(acc: Seq[InternalPlanDescription], plan: InternalPlanDescription):
    Seq[InternalPlanDescription] = {
      plan.children match {
        case SingleChild(inner) if otherFields(plan).isEmpty && otherFields(inner).isEmpty && inner.name == plan.name =>
          compactPlanAcc(acc :+ plan, inner)
        case _ => acc :+ plan
      }
    }
    val similar = compactPlanAcc(Seq.empty[InternalPlanDescription], plan)
    CompactedPlanDescription.create(similar)
  }

  private def details(description: InternalPlanDescription)(implicit columns: mutable.Map[String,Int]): Predef.Map[String, Justified] = description.arguments.flatMap {
    case EstimatedRows(count) => mapping(ESTIMATED_ROWS, Right(format(count)))
    case Rows(count) => mapping(ROWS, Right(count.toString))
    case DbHits(count) => mapping(HITS, Right(count.toString))
    case PageCacheHits(count) => mapping(PAGE_CACHE_HITS, Right(count.toString))
    case PageCacheMisses(count) => mapping(PAGE_CACHE_MISSES, Right(count.toString))
    case PageCacheHitRatio(ratio) => mapping(PAGE_CACHE_HIT_RATIO, Right("%.4f".format(ratio)))
    case Time(nanos) => mapping(TIME, Right("%.3f".format(nanos/1000000.0)))
    case _ => None
  }.toMap + (
    OTHER -> Left(other(description)))

  private def mapping(key: String, value: Justified)(implicit columns: mutable.Map[String,Int]) = {
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
        !x.isInstanceOf[PageCacheHits] &&
        !x.isInstanceOf[PageCacheMisses] &&
        !x.isInstanceOf[PageCacheHitRatio] &&
        !x.isInstanceOf[EstimatedRows] &&
        !x.isInstanceOf[Planner] &&
        !x.isInstanceOf[PlannerImpl] &&
        !x.isInstanceOf[Runtime] &&
        !x.isInstanceOf[SourceCode] &&
        !x.isInstanceOf[ByteCode] &&
        !x.isInstanceOf[Time] &&
        !x.isInstanceOf[RuntimeImpl] &&
        !x.isInstanceOf[Version] => PlanDescriptionArgumentSerializer.serialize(x)
    }
  }

  private def other(description: InternalPlanDescription)(implicit columns: mutable.Map[String,Int]): String = {
    val result = otherFields(description).mkString("; ").replaceAll(UNNAMED_PATTERN, "")
    if (result.nonEmpty) {
      update(columns, OTHER, result.length)
    }
    result
  }

  private def format(v: Double) = if (v.isNaN) v.toString else math.round(v).toString
}

trait LineDetails extends ((String) => Justified) {
  def connection: Option[String]
}

case class Line(tree: String, details: Map[String, Justified], variables: Set[String], connection: Option[String] =
None) extends LineDetails {
  def apply(key: String): Justified = if (key == renderAsTreeTable.OPERATOR) {
    Left(tree)
  } else {
    details.getOrElse(key, Left(""))
  }
}

case class CompactedLine(line: Line, repeated: Set[String]) extends LineDetails {
  val varSep = ", "
  val typeSep = " -- "
  val suffix = ", ..."
  val formattedVariables: String = formatVariables(renderAsTreeTable.MAX_VARIABLE_COLUMN_WIDTH)

  def apply(key: String): Justified = if (key == renderAsTreeTable.VARIABLES)
    Left(formattedVariables)
  else
    line(key)

  def connection: Option[String] = line.connection

  private def formattedVars(vars: List[String], prefix: String = "") = vars match {
    case v :: Nil => List(prefix + v)
    case v :: tail => List(prefix + v) ++ tail.map(v => varSep + v)
    case _ => vars
  }

  def formatVariables(length: Int): String = {
    val newVars = (line.variables -- repeated).toList.sorted.map(PlanDescriptionArgumentSerializer.removeGeneratedNames)
    val oldVars = repeated.toList.sorted.map(PlanDescriptionArgumentSerializer.removeGeneratedNames)
    val all = if(newVars.nonEmpty)
      formattedVars(newVars) ++ formattedVars(oldVars, typeSep)
    else
      formattedVars(oldVars)
    all.length match {
      case 0 => ""
      case 1 => all.head
      case _ => all.head + formatting(all.tail, length - all.head.length)
    }
  }

  private def formatWithTail(variable: String, tail: List[String], length: Int) =
    if (variable.length + tail.head.length + (if (tail.length > 1) suffix.length else 0) <= length)
      variable + formatting(tail, length - variable.length)
    else if (variable.length + suffix.length <= length)
      variable + suffix
    else
      suffix

  private def formatting(variables: List[String], length: Int): String =
    variables match {
      case variable :: Nil =>
        if (variable.length <= length)
          variable
        else
          suffix
      case variable :: tail => formatWithTail(variable, tail, length)
      case _ => ""
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
