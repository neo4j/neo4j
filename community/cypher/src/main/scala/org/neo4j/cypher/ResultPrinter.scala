/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import internal.commands.expressions.StringHelper
import internal.StringExtras
import java.io.PrintWriter
import collection.Map

trait ResultPrinter
  extends StringExtras with StringHelper {

  def dumpToString(writer: PrintWriter, columns: List[String], data: List[Map[String, Any]], timeTaken: Int, queryStatistics : QueryStatistics) {
    val columnSizes = calculateColumnSizes(columns,data)

    if (columns.nonEmpty) {
      val headers = columns.map((c) => Map[String, Any](c -> Some(c))).reduceLeft(_ ++ _)
      val headerLine: String = createString(columns, columnSizes, headers)
      val lineWidth: Int = headerLine.length - 2
      val --- = "+" + repeat("-", lineWidth) + "+"

      val row = if (data.size > 1) "rows" else "row"
      val footer = "%d %s".format(data.size, row)

      writer.println(---)
      writer.println(headerLine)
      writer.println(---)

      data.foreach(resultLine => writer.println(createString(columns, columnSizes, resultLine)))

      writer.println(---)
      writer.println(footer)
      if (queryStatistics.containsUpdates) {
        writer.print(queryStatistics.toString)
      }
    } else {
      if (queryStatistics.containsUpdates) {
        writer.println("+-------------------+")
        writer.println("| No data returned. |")
        writer.println("+-------------------+")
        writer.print(queryStatistics.toString)
      } else {
        writer.println("+--------------------------------------------+")
        writer.println("| No data returned, and nothing was changed. |")
        writer.println("+--------------------------------------------+")
      }
    }

    writer.println("%d ms".format(timeTaken))
  }

  private def calculateColumnSizes(columns : List[String], result: Seq[Map[String, Any]]): Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int] ++ columns.map(name => name -> name.size)

    result.foreach((m) => {
      m.foreach((kv) => {
        val length = text(kv._2).size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  private def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String = {
    columns.map(c => {
      val length = columnSizes.get(c).get
      val txt = text(m.get(c).get)
      val value = makeSize(txt, length)
      value
    }).mkString("| ", " | ", " |")
  }

  def text(x : Any) : String
}