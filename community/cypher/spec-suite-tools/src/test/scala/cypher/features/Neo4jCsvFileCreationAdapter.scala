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
package cypher.features

import org.neo4j.cypher.internal.runtime.TempFileCreator
import org.opencypher.tools.tck.api.CsvFileCreationSupport
import org.opencypher.tools.tck.api.CypherValueRecords
import org.opencypher.tools.tck.api.Graph

import java.io.PrintWriter

trait Neo4jCsvFileCreationAdapter extends CsvFileCreationSupport with TempFileCreator {
  self: Graph =>

  private val csvSeparator: String = ","

  private def printCypherValueRecordsAsCSV(contents: CypherValueRecords)(writer: PrintWriter): Unit = {
    writer.println(contents.header.mkString(csvSeparator))

    contents.rows
      .map(row => contents.header.map(column => row(column)))
      .foreach(orderedRow => writer.println(orderedRow.map(TCKValueToNeo4jValue(_)).mkString(csvSeparator)))
  }

  override def createCSVFile(contents: CypherValueRecords): String = {
    createCSVTempFileURL {
      printCypherValueRecordsAsCSV(contents)
    }
  }
}
