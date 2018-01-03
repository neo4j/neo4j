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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}
import org.junit.Test
import java.io.File
import org.junit.Assert._

class LoadCSVTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {
  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  implicit var csvFilesDir: File = createDir(dir, "csv-files")

  def section = "Load CSV"

  private val artist = new CsvFile("artists.csv").withContentsF(
    Seq("1", "ABBA", "1992"),
    Seq("2", "Roxette", "1986"),
    Seq("3", "Europe", "1979"),
    Seq("4", "The Cardigans", "1992")
  )

  private val artistWithHeaders = new CsvFile("artists-with-headers.csv").withContentsF(
    Seq("Id", "Name", "Year"),
    Seq("1", "ABBA", "1992"),
    Seq("2", "Roxette", "1986"),
    Seq("3", "Europe", "1979"),
    Seq("4", "The Cardigans", "1992")
  )

  private val artistFieldTerminator = new CsvFile("artists-fieldterminator.csv", ';').withContentsF(
    Seq("1", "ABBA", "1992"),
    Seq("2", "Roxette", "1986"),
    Seq("3", "Europe", "1979"),
    Seq("4", "The Cardigans", "1992")
  )

  private val artistsWithEscapeChar = new CsvFile("artists-with-escaped-char.csv").withContentsF(
    Seq("1", "The \"\"Symbol\"\"", "1992")
  )

  filePaths = Map(
    "%ARTIST%" -> CsvFile.urify(artist),
    "%ARTIS_WITH_HEADER%" -> CsvFile.urify(artistWithHeaders),
    "%ARTIST_WITH_FIELD_DELIMITER%" -> CsvFile.urify(artistFieldTerminator),
    "%ARTIST_WITH_ESCAPE_CHAR%" -> CsvFile.urify(artistsWithEscapeChar)
  )

  urls = Map(
    "%ARTIST%" -> (baseUrl + artist.getName),
    "%ARTIS_WITH_HEADER%" -> (baseUrl + artistWithHeaders.getName),
    "%ARTIST_WITH_FIELD_DELIMITER%" -> (baseUrl + artistFieldTerminator.getName),
    "%ARTIST_WITH_ESCAPE_CHAR%" -> (baseUrl + artistsWithEscapeChar.getName)
  )

  @Test def should_import_data_from_a_csv_file() {
    testQuery(
      title = "Import data from a CSV file",
      text = """
To import data from a CSV file into Neo4j, you can use +LOAD CSV+ to get the data into your query.
Then you write it to your database using the normal updating clauses of Cypher.

.artists.csv
[source]
----
include::csv-files/artists.csv[]
----
""",
      queryText = s"LOAD CSV FROM '%ARTIST%' AS line CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation =
        """
A new node with the +Artist+ label is created for each row in the CSV file.
In addition, two columns from the CSV file are set as properties on the nodes.""",
      assertions = (p) => assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4))
  }

  @Test def should_import_data_from_a_csv_file_with_headers() {
    testQuery(
      title = "Import data from a CSV file containing headers",
      text = """
When your CSV file has headers, you can view each row in the file as a map instead of as an array of strings.

.artists-with-headers.csv
[source]
----
include::csv-files/artists-with-headers.csv[]
----
""",
      queryText = s"LOAD CSV WITH HEADERS FROM '%ARTIS_WITH_HEADER%' AS line CREATE (:Artist {name: line.Name, year: toInt(line.Year)})",
      optionalResultExplanation = """
This time, the file starts with a single row containing column names.
Indicate this using +WITH HEADERS+ and you can access specific fields by their corresponding column name.""",
      assertions = (p) => assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4))
  }

  @Test def should_import_data_from_a_csv_file_with_custom_field_terminator() {
    testQuery(
      title = "Import data from a CSV file with a custom field delimiter",
      text = """
Sometimes, your CSV file has other field delimiters than commas.
You can specify which delimiter your file uses using +FIELDTERMINATOR+.

.artists-fieldterminator.csv
[source]
----
include::csv-files/artists-fieldterminator.csv[]
----
""",
      queryText = s"LOAD CSV FROM '%ARTIST_WITH_FIELD_DELIMITER%' AS line FIELDTERMINATOR ';' CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation =
        "As values in this file are separated by a semicolon, a custom +FIELDTERMINATOR+ is specified in the +LOAD CSV+ clause.",
      assertions = (p) => assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4))
  }

  @Test def should_import_data_from_a_csv_file_with_periodic_commit() {
    testQuery(
      title = "Importing large amounts of data",
      text = """
If the CSV file contains a significant number of rows (approaching hundreds of thousands or millions), +USING PERIODIC COMMIT+
can be used to instruct Neo4j to perform a commit after a number of rows.
This reduces the memory overhead of the transaction state.
By default, the commit will happen every 1000 rows.
For more information, see <<query-periodic-commit>>.
""",
      queryText = s"USING PERIODIC COMMIT LOAD CSV FROM '%ARTIST%' AS line CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4))
  }

  @Test def should_import_data_from_a_csv_file_with_periodic_commit_after_500_rows() {
    testQuery(
      title = "Setting the rate of periodic commits",
      text = """You can set the number of rows as in the example, where it is set to 500 rows.""",
      queryText = s"USING PERIODIC COMMIT 500 LOAD CSV FROM '%ARTIST%' AS line CREATE (:Artist {name: line[1], year: toInt(line[2])})",
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, nodesCreated = 4, propertiesSet = 8, labelsAdded = 4))
  }

  @Test def should_import_data_from_a_csv_file_which_uses_the_escape_char() {
    testQuery(
      title = "Import data containing escaped characters",
      text = """
In this example, we both have additional quotes around the values, as well as escaped quotes inside one value.

.artists-with-escaped-char.csv
[source]
----
include::csv-files/artists-with-escaped-char.csv[]
----
""",
      queryText = s"LOAD CSV FROM '%ARTIST_WITH_ESCAPE_CHAR%' AS line CREATE (a:Artist {name: line[1], year: toInt(line[2])}) return a.name as name, a.year as year, length(a.name) as length",
      optionalResultExplanation = """
Note that strings are wrapped in quotes in the output here.
You can see that when comparing to the length of the string in this case!""",
      assertions = (p) => assertEquals(List(Map("name" -> """The "Symbol"""", "year" -> 1992, "length" -> 12)), p.toList)
    )
  }
}
