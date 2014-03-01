/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}
import org.junit.Test
import java.io.File

class LoadCSVTest extends DocumentingTestBase with QueryStatisticsTestSupport {
  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  implicit var csvFilesDir: File = createDir(dir, "csv-files")

  def section = "Load CSV"

  @Test def should_import_data_from_a_csv_file() {
    val url = new CsvFile("file.csv").withContents(
      Seq("1", "ABBA", "1992"),
      Seq("2", "Roxette", "1986"),
      Seq("3", "Europe", "1979"),
      Seq("4", "The Cardigans", "1992")
    )

    testQuery(
      title = "Import data from a CSV file",
      text = "Load CSV allows to import data into Neo4j from a csv file.",
      queryText = s"LOAD CSV FROM '${url}' AS line CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation =
        "A new node with the Artist label is created for each row in the CSV file. In addition, two columns " +
        "from the CSV file are set as properties on the nodes.",
      assertions = (p) => { assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4) })
  }

  @Test def should_import_data_from_a_csv_file_with_headers() {
    val url = new CsvFile("file.csv").withContents(
      Seq("Id", "Name", "Year"),
      Seq("1", "ABBA", "1992"),
      Seq("2", "Roxette", "1986"),
      Seq("3", "Europe", "1979"),
      Seq("4", "The Cardigans", "1992")
    )

    testQuery(
      title = "Import data from a CSV file containing headers",
      text = "Load CSV allows to import data into Neo4j from a csv file.",
      queryText = s"LOAD CSV WITH HEADERS FROM '${url}' AS line CREATE (:Artist {name: line.Name, year: toInt(line.Year)})",
      optionalResultExplanation =
        "This time, the file starts with a single row containing column names and WITH HEADERS allows you to directly " +
        "access specific fields by their corresponding column name",
      assertions = (p) => { assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4) })
  }

  @Test def should_import_data_from_a_csv_file_with_custom_field_terminator() {
    val url = new CsvFile("file.csv", ';').withContents(
      Seq("1", "ABBA", "1992"),
      Seq("2", "Roxette", "1986"),
      Seq("3", "Europe", "1979"),
      Seq("4", "The Cardigans", "1992")
    )

    testQuery(
      title = "Import data from a CSV file with a ';' field delimiter",
      text = "Load CSV allows to import data into Neo4j from a csv file.",
      queryText = s"LOAD CSV FROM '${url}' AS line FIELDTERMINATOR ';' CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation =
        "As values in this file are separated by a semicolon, a custom FIELDTERMINATOR is specified in the LOAD CSV clause.",
      assertions = (p) => { assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4) })
  }

  @Test def should_import_data_from_a_csv_file_with_periodic_commit() {
    val url = new CsvFile("file.csv").withContents(
      Seq("1", "ABBA", "1992"),
      Seq("2", "Roxette", "1986"),
      Seq("3", "Europe", "1979"),
      Seq("4", "The Cardigans", "1992")
    )

    testQuery(
      title = "Import data from a CSV file with a ';' field delimiter",
      text = "Load CSV allows to import data into Neo4j from a csv file.",
      queryText = s"USING PERIODIC COMMIT LOAD CSV FROM '${url}' AS line CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation =
        "Here, if the file contains a significant number of rows (approaching hundreds of thousands or millions), USING PERIODIC COMMIT " +
        "can be used to instruct Neo4j to perform a commit multiple times at a certain interval (defaults to 10000 updates) " +
        "so as to reduce the memory overhead of the transaction state.",
      assertions = (p) => { assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4) })
  }
}
