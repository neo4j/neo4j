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

import org.neo4j.cypher.{ExecutionResult, StatisticsChecker}
import java.io.File

class LoadCSVWithExplicitJoinTableTest extends ArticleTest with StatisticsChecker {
  implicit var csvFilesDir: File = _

  override def doThisBefore() {
    csvFilesDir = createDir(dir, "csv-files")
  }

  def title: String = "Importing data from multiple CSV files"
  def section: String = "Import"
  def assert(name: String, result: ExecutionResult) {}
  def graphDescription: List[String] = List.empty

  val movies = create("movies.csv") withContents(
    Seq("1", "Wall Street"),
    Seq("2", "The American President"))

  val persons = create("persons.csv") withContents(
    Seq("1", "Charlie Sheen"),
    Seq("2", "Oliver Stone"),
    Seq("3", "Michael Douglas"),
    Seq("4", "Rob Reiner"),
    Seq("5", "Martin Sheen"))

  val directors = create("directors.csv") withContents(
    Seq("2", "1"),
    Seq("4", "2"))

  private def create(fileName: String): CsvFile = new CsvFile(fileName)

  def text: String = s"""= $title =
                       |This tutorial will show you how to import data from multiple CSV files using +LOAD CSV+.
                       |These files might be the result of exporting tables from a relational database and might
                       |contain explicit join keys.
                       |
                       |In this example, we'll use three files, one listing people, one listing movies and another one
                       |describing which movies have been directed by whom.
                       |All the rows in movies.csv and persons.csv will have an explicit primary key as you may expect
                       |in a relational database. The directors.csv mapping will consists of pairs of said keys.
                       |
                       |We'll use the following steps to import those three CSV files to the graph.
                       |
                       |Initially, we'll create indices for the id property on Person and Movie nodes. The id property
                       |will be a temporary property used to look up the appropriate nodes for a relationship when
                       |importing the third file. By creating an index, the +MATCH+ look ups will be much faster.
                       |
                       |###
                       |CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
                       |###
                       |###
                       |CREATE CONSTRAINT ON (m:Movie) ASSERT m.id IS UNIQUE
                       |###
                       |
                       |First, we import the movies.csv file and create a Movie node for each row.
                       |
                       |###
                       |LOAD CSV FROM "file://$movies" AS csvLine
                       |CREATE (m:Movie {id: toInt(csvLine[0]), title: csvLine[1]})
                       |###
                       |
                       |Second, we do the same for the persons.csv file.
                       |
                       |###
                       |LOAD CSV FROM "file://$persons" AS csvLine
                       |CREATE (p:Person {id: toInt(csvLine[0]), name: csvLine[1]})
                       |###
                       |
                       |The last step is to create the relationships between nodes we've just created. Given a
                       |pair of two IDs, we look up the corresponding nodes and create a +DIRECTED+ relationship
                       |between them.
                       |
                       |###
                       |LOAD CSV FROM "file://$directors" AS csvLine
                       |MATCH (p:Person {id: toInt(csvLine[0])}), (m:Movie {id: csvLine[1]})
                       |CREATE (p)-[:DIRECTED]->(m)
                       |###
                       |
                       |Finally, as the id property was only necessary to import the relationships, we can drop the
                       |constraints and the property from all nodes.
                       |
                       |###
                       |DROP CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE
                       |###
                       |###
                       |DROP CONSTRAINT ON (m:Movie) ASSERT m.id IS UNIQUE
                       |###
                       |###
                       |MATCH (n)
                       |REMOVE n.id
                       |###
                       |""".stripMargin
}
