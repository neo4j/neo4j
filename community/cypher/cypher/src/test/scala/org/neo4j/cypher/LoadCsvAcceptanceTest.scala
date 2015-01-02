/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.{Ignore, After, Test}
import java.io.PrintWriter
import scala.reflect.io.File

@Ignore("Disabling LOAD CSV in 2.0, Jakub")
class LoadCsvAcceptanceTest extends ExecutionEngineJUnitSuite with QueryStatisticsTestSupport {
  @Test def import_three_strings() {
    val url = createFile {
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    }

    val result = execute(s"LOAD CSV FROM '${url}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  @Test def import_three_numbers() {
    val url = createFile {
      writer =>
        writer.println("1")
        writer.println("2")
        writer.println("3")
    }

    val result =
      execute(s"LOAD CSV FROM '${url}' AS line CREATE (a {number: line[0]}) RETURN a.number")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)

    result.columnAs[Long]("a.number").toList === List("")
  }

  @Test def import_three_rows_numbers_and_strings() {
    val url = createFile {
      writer =>
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    }

    val result = execute(s"LOAD CSV FROM '${url}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 3)
  }

  @Test def import_three_rows_with_headers() {
    val url = createFile {
      writer =>
        writer.println("id,name")
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    }

    val result = execute(s"LOAD CSV WITH HEADERS FROM '${url}' AS line CREATE (a {id: line.id, name: line.name}) RETURN a.name")
    assertStats(result, nodesCreated = 3, propertiesSet = 6)
  }

  @Test def import_three_rows_with_headers_messy_data() {
    val url = createFile {
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aadvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
    }

    val result = execute(s"LOAD CSV WITH HEADERS FROM '${url}' AS line RETURN line.x")
    assert(result.toList === List(Map("line.x" -> "0"), Map("line.x" -> null), Map("line.x" -> "1")))
  }

  @Test def should_handle_quotes() {
    val url = createFile {
      writer =>
        writer.println("String without quotes")
        writer.println("'String, with single quotes'")
        writer.println("\"String, with double quotes\"")
        writer.println(""""String with ""quotes"" in it"""")
        writer.println("""String with "quotes" in it""")
    }

    val result = execute(s"LOAD CSV FROM '${url}' AS line RETURN line as string").toList
    assert(result === List(
      Map("string" -> Seq("String without quotes")),
      Map("string" -> Seq("'String", " with single quotes'")),
      Map("string" -> Seq("String, with double quotes")),
      Map("string" -> Seq( """String with "quotes" in it""")),
      Map("string" -> Seq( """String with "quotes" in it"""))))
  }

  @Test def empty_file_does_not_create_anything() {
    val url = createFile(writer => {})

    val result = execute(s"LOAD CSV FROM '${url}' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 0)
  }

  var files: Seq[File] = Seq.empty

  private def createFile(f: PrintWriter => Unit): String = synchronized {
    val file = File.makeTemp("cypher", ".csv")
    val writer = file.printWriter()
    f(writer)
    writer.flush()
    writer.close()
    files = files :+ file
    file.toURI.toURL.toString.replace("\\", "\\\\")
  }

  @After def cleanup() {
    files.foreach(_.delete())
  }
}
