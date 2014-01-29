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
package org.neo4j.cypher

import org.junit.{After, Test}
import java.io.PrintWriter
import scala.reflect.io.File

class LoadCsvAcceptanceTest extends ExecutionEngineHelper with StatisticsChecker {
  @Test def import_three_strings() {
    val fileName = createFile {
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    }

    val result = execute(s"LOAD CSV FROM 'file://${fileName}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  @Test def import_three_numbers() {
    val fileName = createFile {
      writer =>
        writer.println("1")
        writer.println("2")
        writer.println("3")
    }

    val result =
      execute(s"LOAD CSV FROM 'file://${fileName}' AS line CREATE (a {number: line[0]}) RETURN a.number")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)

    result.columnAs[Long]("a.name").toList === List("")
  }

  @Test def import_three_rows_numbers_and_strings() {
    val fileName = createFile {
      writer =>
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    }

    val result = execute(s"LOAD CSV FROM 'file://${fileName}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3)
  }

  @Test def empty_file_does_not_create_anything() {
    val fileName = createFile(writer => {})

    val result = execute(s"LOAD CSV FROM 'file://${fileName}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3)
  }

  var files: Seq[File] = Seq.empty

  private def createFile(f: PrintWriter => Unit): String = synchronized {
    val file = File.makeTemp("cypher", ".csv")
    val writer = file.printWriter()
    f(writer)
    writer.flush()
    writer.close()
    files = files :+ file
    file.path
  }

  @After def cleanup() {
    files.foreach(_.delete())
  }
}
