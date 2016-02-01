/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planDescription

import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments._

import scala.Predef
import scala.collection.{Map, mutable}

object renderAsTreeTable extends (InternalPlanDescription => String) {
  private val UNNAMED_PATTERN = """  (UNNAMED|FRESHID|AGGREGATION)\d+"""
  val OPERATOR = "Operator"
  private val ESTIMATED_ROWS = "Estimated Rows"
  private val ROWS = "Rows"
  private val HITS = "DB Hits"
  private val TIME = "Time (ms)"
  val VARIABLES = "Variables"
  private val OTHER = "Other"
  private val HEADERS = Seq(OPERATOR, ESTIMATED_ROWS, ROWS, HITS, TIME, VARIABLES, OTHER)
  val newLine = System.lineSeparator()

  def apply(plan: InternalPlanDescription): String = {

    implicit val columns = new mutable.HashMap[String, Int]()
    val lines = accumulate(plan)

    def compactLine(line: Line, previous: Seq[(Line,Line)]) = {
      val repeatedVariables: Set[String] = if (previous.size > 0) previous.head._1.variables.intersect(line.variables) else Set.empty
      if (repeatedVariables.size > 0) {
        line.copy(variables = line.variables -- repeatedVariables, compacted = true)
      } else
        line
    }
    val compactedLines = lines.reverse.foldLeft(Seq.empty[(Line, Line)]){(acc,line) =>
      val compacted = compactLine(line, acc)
      (line,compacted) +: acc
    } map(_._2)

    val headers = HEADERS.filter(columns.contains)

    def width(header:String) = {
      2 + math.max(header.length, columns(header))
    }

    val result = new StringBuilder((2 + newLine.length + headers.map(width).sum) * (lines.size * 2 + 3))

    def pad(width:Int, char:String=" ") =
      for (i <- 1 to width) result.append(char)
    def divider(line:Line = null) = {
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
    new CompactedPlanDescription(similar).planDescription
  }

  private def details(description: InternalPlanDescription)(implicit columns: mutable.Map[String,Int]): Predef.Map[String, Justified] = description.arguments.flatMap {
    case EstimatedRows(count) => mapping(ESTIMATED_ROWS, Right(format(count)))
    case Rows(count) => mapping(ROWS, Right(count.toString))
    case DbHits(count) => mapping(HITS, Right(count.toString))
    case Time(nanos) => mapping(TIME, Right("%.3f".format(nanos/1000000.0)))
    case _ => None
  }.toMap + (
    VARIABLES -> Left(variables(description)),
    OTHER -> Left(other(description)))

  private def mapping(key: String, value: Justified)(implicit columns: mutable.Map[String,Int]) = {
    update(columns, key, value.length)
    Some(key -> value)
  }

  private def update(columns: mutable.Map[String, Int], key: String, length: Int): Unit = {
    columns.put(key, math.max(columns.getOrElse(key, 0), length))
  }

  private def variables(description: InternalPlanDescription)(implicit columns: mutable.Map[String,Int]): String = {
    val result: String = description.orderedVariables.map(PlanDescriptionArgumentSerializer.removeGeneratedNames).mkString(", ")
    if (result.nonEmpty) {
      update(columns, VARIABLES, result.length)
    }
    result
  }

  private def otherFields(description: InternalPlanDescription) = {
    description.arguments.collect { case x
      if !x.isInstanceOf[Rows] &&
        !x.isInstanceOf[DbHits] &&
        !x.isInstanceOf[EstimatedRows] &&
        !x.isInstanceOf[Planner] &&
        !x.isInstanceOf[PlannerImpl] &&
        !x.isInstanceOf[Runtime] &&
        !x.isInstanceOf[SourceCode] &&
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

case class Line(tree: String, details: Map[String, Justified], variables: Set[String], connection: Option[String] = None, compacted: Boolean = false) {
  def apply(key: String): Justified = if (key == renderAsTreeTable.OPERATOR) {
    Left(tree)
  } else if (key == renderAsTreeTable.VARIABLES) {
    Left(variables.toSeq.sorted.map(PlanDescriptionArgumentSerializer.removeGeneratedNames).mkString(", ") + (if(compacted && variables.size > 0) ", ..." else ""))
  } else {
    details.getOrElse(key, Left(""))
  }
}

sealed abstract class Justified(text:String) {
  def length = text.length
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
