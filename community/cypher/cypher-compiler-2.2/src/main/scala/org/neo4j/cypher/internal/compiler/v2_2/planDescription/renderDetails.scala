/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planDescription

import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments._

import scala.collection.mutable


object renderDetails extends (InternalPlanDescription => String) {
  val UNNAMED_PATTERN = """  (UNNAMED|FRESHID|AGGREGATION)\d+"""

  def apply(plan: InternalPlanDescription): String = {
    val plans: Seq[InternalPlanDescription] = plan.flatten
    val names = renderAsTree.createUniqueNames(plan)


    val headers = Seq("Operator", "EstimatedRows", "Rows", "DbHits", "Identifiers", "Other")
    val rows: Seq[Seq[(String, Option[String])]] = plans.map {
      p =>
        val name = Some(names(p))
        val rows = p.arguments.collectFirst { case Rows(count) => count.toString}
        val estimatedRows = p.arguments.collectFirst { case EstimatedRows(count) => format(count) }
        val dbHits = p.arguments.collectFirst { case DbHits(count) => count.toString}
        val ids = Some(p.orderedIdentifiers.map(PlanDescriptionArgumentSerializer.removeGeneratedNames).mkString(", "))
        val other = Some(p.arguments.collect {
          case x
            if !x.isInstanceOf[Rows] &&
              !x.isInstanceOf[DbHits] &&
              !x.isInstanceOf[EstimatedRows] &&
              !x.isInstanceOf[Planner] &&
              !x.isInstanceOf[Version] => PlanDescriptionArgumentSerializer.serialize(x)
        }.mkString("; ")
          .replaceAll(UNNAMED_PATTERN, ""))

        Seq("Operator" -> name, "EstimatedRows" -> estimatedRows, "Rows" -> rows,
          "DbHits" -> dbHits, "Identifiers" -> ids, "Other" -> other)
    }

    //Remove headers where no values are available
    val headersInUse = rows.flatten.groupBy(_._1).filter {
      case (_, seq) => !seq.forall(_._2 == None)
    }.keySet

    val rowsInUse =
      rows.map(row => row.filter(r => headersInUse.contains(r._1)).map(_._2.getOrElse("-")))

    renderTable(headers.filter(headersInUse.contains), rowsInUse)
  }

  private def format(v: Double) = if (v.isNaN) v.toString else math.round(v).toString


  private def renderTable(header: Seq[String], rows: Seq[Seq[String]]): String = {
    val columnWidth = mutable.ArrayBuffer[Int](header.map(_.length): _*)
    rows.foreach {
      (row: Seq[String]) =>
        row.zipWithIndex foreach {
          case (cell, index) => if (columnWidth(index) < cell.length) columnWidth(index) = cell.length
        }
    }

    val builder = new StringBuilder()

    def addHorizontalLine() {
      columnWidth.foreach {
        width =>
          builder.append("+").append("-" * (width + 2))
      }
    }

    def addLine(cells: Seq[String]) {
      cells zip columnWidth foreach {
        case (cell: String, width: Int) =>
          val length = cell.length
          builder.append("| " + " " * ((width + 1) - length - 1) + cell + " ")
      }
    }

    def newLine() {
      builder.append("%n".format())
    }

    addHorizontalLine()
    builder.append("+")
    newLine()
    addLine(header)
    builder.append("|")
    newLine()
    addHorizontalLine()
    builder.append("+")
    newLine()

    rows.foreach {
      (line: Seq[String]) =>
        addLine(line)
        builder.append("|")
        newLine()
    }

    addHorizontalLine()
    builder.append("+")
    newLine()

    builder.toString()
  }
}
