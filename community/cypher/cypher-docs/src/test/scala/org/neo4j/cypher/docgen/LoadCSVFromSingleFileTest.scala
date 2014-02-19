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

import org.neo4j.cypher.{ExecutionResult, QueryStatisticsTestSupport}
import java.io.File

class LoadCSVFromSingleFileTest extends ArticleTest with QueryStatisticsTestSupport {
  implicit var csvFilesDir: File = _
  private var roles:String = _
  private var movie_productions:String = _

  override def doThisBefore() {
    csvFilesDir = createDir(dir, "csv-files")

    roles = create("roles.csv") withContents(
      Seq("Charlie Sheen", "Wall Street", "Bud Fox"),
      Seq("Charlie Sheen", "Wall Street", "Bud Fox"),
      Seq("Martin Sheen", "Wall Street", "Carl Fox"),
      Seq("Michael Douglas", "Wall Street", "Gordon Gekko"),
      Seq("Martin Sheen", "The American President", "A.J. MacInerney"),
      Seq("Michael Douglas", "The American President", "President Andrew Shepherd"))

    movie_productions = create("movie_productions.csv") withContents(
      Seq("movie", "country", "year"),
      Seq("Cloud Atlas", "Germany", "2012"),
      Seq("Cloud Atlas", "USA", "2012"),
      Seq("The Shawshank Redemption", "USA", "1993"))
  }

  def title: String = "Importing data from a single CSV file"
  def section: String = "Import"
  def assert(name: String, result: ExecutionResult) {}
  def graphDescription: List[String] = List.empty


  private def create(fileName: String): CsvFile = new CsvFile(fileName)

  def text: String = s"""= $title =
                       |This tutorial will show you how to import data from a single file using +LOAD CSV+.
                       |
                       |In this example, we're given a CSV file where each line contains a mapping of an actor, a movie
                       |they've played in and the role they were cast as in that movie.  Below we will assume that
                       |the CSV file has been stored on the database server and access it using a +file://+ URL.
                       |Alternatively, +LOADCSV+ also supports accessing CSV files via +HTTPS+, +HTTP+, and +FTP+.
                       |
                       |Using the following Cypher query, we'll create a node for each movie, a node for each actor and
                       |a relationship between the two with a property denoting the character name. We're using +MERGE+
                       |for creating the nodes so as to avoid creating duplicate nodes in the case where the same actor
                       |or movie appears in the file multiple times.
                       |
                       |If the input CSV file is big enough, a good way of speeding up the +MERGE+ commands is by
                       |creating two indices for Person nodes on the name property and Movies nodes on the title
                       |property before the +LOAD CSV+ command.
                       |
                       |###
                       |CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE###
                       |
                       |###
                       |CREATE CONSTRAINT ON (m:Movie) ASSERT m.title IS UNIQUE###
                       |
                       |###
                       |LOAD CSV FROM "$roles" AS csvLine
                       |MERGE (p:Person {name: csvLine[0]})
                       |MERGE (m:Movie {title: csvLine[1]})
                       |CREATE (p)-[:PLAYED {role: csvLine[2]}]->(m)###
                       |
                       |+LOAD CSV+ also supports loading CSV files that start with headers (column names). In this
                       |case these header names may be used to address the values in a row via the
                       |+LOAD CSV WITH HEADERS+ clause.
                       |
                       |== Using PERIODIC COMMIT ==
                       |
                       |When importing lots of data, the transaction state might build up so much that queries run very
                       |slowly or even crash the JVM. By using +PERIODIC COMMIT+, the transaction will be committed at
                       |periodic intervals.
                       |
                       |All you need to do is start your query with the +PERIODIC COMMIT+ hint, like so:
                       |###
                       |USING PERIODIC COMMIT
                       |LOAD CSV WITH HEADERS FROM "$movie_productions" AS csvLine
                       |MERGE (c:Country {name: csvLine.country})
                       |MERGE (m:Movie {title: csvLine.movie})
                       |CREATE (p)-[:PRODUCED {year: toInt(csvLine.year)}]->(m)###
                       |
                       |+LOAD CSV+ produces values that are collections (or maps when +WITH HEADERS+ is used) of strings.
                       |In order to convert them to appropriate types, use the built-in +toInt+ and +toFloat+ functions.
                       |""".stripMargin

}
