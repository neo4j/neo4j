/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import java.io.{PrintWriter, StringWriter}

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.StringHelper
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext

case class ExecutionResultDumper(result: Seq[Map[String, Any]], columns: List[String], queryStatistics: InternalQueryStatistics) extends StringHelper {

  def dumpToString(implicit query: QueryContext): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  def dumpToString(writer: PrintWriter)(implicit query: QueryContext) {
    if (columns.nonEmpty) {
      val headers = columns.map((c) => Map[String, Any](c -> Some(c))).reduceLeft(_ ++ _)
      val columnSizes = calculateColumnSizes
      val headerLine = createString(columnSizes, headers)
      val lineWidth = headerLine.length - 2
      val --- = "+" + repeat("-", lineWidth) + "+"

      val row = if (result.size > 1) "rows" else "row"
      val footer = "%d %s".format(result.size, row)

      writer.println(---)
      writer.println(headerLine)
      writer.println(---)

      result.foreach(resultLine => writer.println(createString(columnSizes, resultLine)))

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
  }

  def createString(columnSizes: Map[String, Int], m: Map[String, Any])(implicit query: QueryContext): String = {
    columns.map(c => {
      val length = columnSizes.get(c).get
      val txt = text(m.get(c).get, query)
      val value = makeSize(txt, length)
      value
    }).mkString("| ", " | ", " |")
  }

  def calculateColumnSizes(implicit query: QueryContext): Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.OpenHashMap[String, Int] ++ columns.map(name => name -> name.size)

    result.foreach((m) => {
      m.foreach((kv) => {
        val length = text(kv._2, query).size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }
}

