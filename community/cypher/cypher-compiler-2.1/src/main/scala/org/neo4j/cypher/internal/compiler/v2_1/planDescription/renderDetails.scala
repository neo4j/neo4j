/**
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
package org.neo4j.cypher.internal.compiler.v2_1.planDescription

import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments._

import scala.collection.mutable

object renderDetails extends (PlanDescription => String) {

  val handledArguments = Set(classOf[Rows], classOf[DbHits], classOf[IntroducedIdentifier])

  def apply(plan: PlanDescription): String = {

    val plans: Seq[PlanDescription] = plan.toSeq
    val names = renderAsTree.createUniqueNames(plan)

    val headers = Seq("Operator", "Rows", "DbHits", "Identifiers", "Other")
    val rows = plans.map {
      p =>
        val name: String = names(p)
        val rows: String = p.arguments.collectFirst { case Rows(count) => count.toString }.getOrElse("?")
        val dbHits: String = p.arguments.collectFirst { case DbHits(count) => count.toString }.getOrElse("?")
        val ids: String = p.arguments.collect { case IntroducedIdentifier(id) => id }.mkString(", ")
        val other = p.arguments.collect {
          case x
            if !x.isInstanceOf[Rows] &&
              !x.isInstanceOf[DbHits] &&
              !x.isInstanceOf[IntroducedIdentifier] => PlandescriptionArgumentSerializer.serialize(x)
        }.mkString("; ")

        Seq(name, rows, dbHits, ids, other)
    }

    renderTable(headers, rows)
  }

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
