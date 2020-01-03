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
package org.neo4j.cypher.internal.result.string

import org.neo4j.graphdb.QueryStatistics

/**
 * Creates formatted tabular output.
 */
object FormatOutput {

  def format(writer: FormatOutputWriter,
             columns: Array[String],
             result: Seq[Array[String]],
             queryStatistics: QueryStatistics): Unit = {

    def makeSize(str: String, wantedSize: Int): String = {
      val actualSize = str.length()
      if (actualSize > wantedSize) {
        str.slice(0, wantedSize)
      } else if (actualSize < wantedSize) {
        str.padTo(wantedSize, ' ')
      } else str
    }

    def repeat(x: String, size: Int): String = (1 to size).map(i => x).mkString

    def createLine(columnSizes: Array[Int], row: Array[String]): String = {
      columnSizes.indices.map(i => {
        val length = columnSizes(i)
        val valueString = row(i)
        makeSize(valueString, length)
      }).mkString("| ", " | ", " |")
    }

    def calculateColumnSizes(result: Seq[Array[String]]): Array[Int] = {
      val columnSizes = columns.map(_.length)

      for {
        row <- result
        i <- columns.indices
      } {
        val column = columns(i)
        val valueLength = row(i).length
        if (columnSizes(i) < valueLength)
          columnSizes(i) = valueLength
      }

      columnSizes
    }

    if (columns.nonEmpty) {
      val columnSizes = calculateColumnSizes(result)
      val headerLine = createLine(columnSizes, columns)
      val lineWidth = headerLine.length - 2
      val --- = "+".padTo(lineWidth + 1, '-') + "+"

      val row = if (result.size > 1) "rows" else "row"
      val footer = "%d %s".format(result.size, row)

      writer.println(---)
      writer.println(headerLine)
      writer.println(---)

      result.foreach(resultLine => writer.println(createLine(columnSizes, resultLine)))

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


}
