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
import java.io.{PrintWriter, File}

class LoadCSVTest extends ArticleTest with StatisticsChecker {
  implicit val csvFilesDir: File = createDir(dir, "csv-files")

  def title: String = "Importing data with LOAD CSV"
  def section: String = "Importing data with LOAD CSV"
  def assert(name: String, result: ExecutionResult) {}
  def graphDescription: List[String] = List.empty

  val movies = create("movies.csv") withContents(
    Seq("Wall Street"),
    Seq("The American President"))

  val persons = create("persons.csv") withContents(
    Seq("Charlie Sheen"),
    Seq("Oliver Stone"),
    Seq("Michael Douglas"),
    Seq("Rob Reiner"),
    Seq("Martin Sheen"))

  val roles = create("roles.csv") withContents(
    Seq("Charlie Sheen", "Wall Street", "Bud Fox"),
    Seq("Martin Sheen", "Wall Street", "Carl Fox"),
    Seq("Michael Douglas", "Wall Street", "Gordon Gekko"),
    Seq("Martin Sheen", "The American President", "A.J. MacInerney"),
    Seq("Michael Douglas", "The American President", "President Andrew Shepherd"))

  val directors = create("directors.csv") withContents(
    Seq("Oliver Stone", "Wall Street"),
    Seq("Rob Reiner", "The American President"))

  private def create(fileName: String): CsvFile = new CsvFile(fileName)

  def text: String = """Importing Data from CSV
                       |=======================
                       |This tutorial will show you how to import a dataset using +LOAD CSV+.
                       |
                       |== Patterns for nodes ==
                       |
                       |###
                       |load csv from "file://""" + persons + """" as line return line[0]###""".stripMargin
}

class CsvFile(fileName: String)(implicit csvFilesDir: File) {
  def withContents(lines: Seq[String]*): String = {
    val csvFile = new File(csvFilesDir, fileName)
    val writer = new PrintWriter(csvFile, "UTF-8")
    lines.foreach(line => writer.println(line.mkString("\"", "\",\"", "\"")))
    writer.flush()
    writer.close()

    csvFile.getAbsolutePath
  }
}
