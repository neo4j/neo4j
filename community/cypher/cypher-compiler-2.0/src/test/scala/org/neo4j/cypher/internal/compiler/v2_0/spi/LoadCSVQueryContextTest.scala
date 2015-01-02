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
package org.neo4j.cypher.internal.compiler.v2_0.spi

import org.scalatest.{BeforeAndAfter, Matchers}
import java.io.PrintWriter
import scala.reflect.io.File
import java.net.URL
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_0.CleanUpper

class LoadCSVQueryContextTest extends CypherFunSuite with Matchers with MockitoSugar with BeforeAndAfter {

  var ctx: LoadCSVQueryContext = _
  var inner: QueryContext = _

  before {
    inner = mock[QueryContext]
    ctx = new LoadCSVQueryContext(inner)
  }

  test("should_handle_strings") {
    // given
    val url = createFile { writer =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
    }

    //when
    val result: List[Array[String]] = ctx.getCsvIterator(url).toList

    (result zip List(
      Array[String] ("1"),
      Array[String] ("2"),
      Array[String] ("3"),
      Array[String] ("4")
    )).foreach {
        case (r, expected) =>
          r should equal (expected)
    }
  }

  test("should handle with headers") {
    // given
    val url = createFile { writer =>
      writer.println("a,b")
      writer.println("1,2")
      writer.println("3,4")
    }

    //when
    val result = ctx.getCsvIterator(url).toList

    //then
    (result zip List(
      Array[String]("a", "b"),
      Array[String]("1", "2"),
      Array[String]("3", "4")
    )).foreach {
      case (r, expected) =>
        r should equal (expected)
    }
  }

  test("should handle with headers even for uneven files") {
    // given
    val url = createFile {
      writer =>
        writer.println("a,b")
        writer.println("1,2")
        writer.println("3")
    }

    //when
    val result = ctx.getCsvIterator(url).toList

    //then
    (result zip List(
      Array[String]("a", "b"),
      Array[String]("1", "2"),
      Array[String]("3")
    )).foreach {
      case (r, expected) =>
        r should equal (expected)
    }
  }

  test("should give a helpful message when asking for headers with empty file") {
    // given
    val url = createFile(_ => {})

    //when
    val result = ctx.getCsvIterator(url).toList

    result should equal (List.empty)
  }

  // 2014-01-30 Andres & Jakub - Tries to get data from internet
  ignore("should be able to handle an http file") {
    // given
    val url = new URL("https://dl.dropboxusercontent.com/u/47132/test.csv")

    //when
    val result = ctx.getCsvIterator(url).toList

    //then
    (result zip List(
      Array[String]("Stefan"),
      Array[String]("Jakub"),
      Array[String]("Andres"),
      Array[String]("Davide"),
      Array[String]("Chris")
    )).foreach {
      case (r, expected) =>
        r should equal (expected)
    }
  }

  test("cleans up csv files even if the inner context throws") {
    when(inner.close(success = true)).thenThrow(new RuntimeException)

    val cleaner = mock[CleanUpper]

    val ctx = new LoadCSVQueryContext(inner, cleaner)
    val e = intercept[RuntimeException](ctx.close(success = true))

    verify(cleaner, times(1)).cleanUp()
  }

  var files: Seq[File] = Seq.empty

  private def createFile(f: PrintWriter => Unit): URL = synchronized {
    val file = File.makeTemp("cypher", ".csv")
    val writer = file.printWriter()
    f(writer)
    writer.flush()
    writer.close()
    files = files :+ file
    file.toURI.toURL
  }

  after {
    files.foreach(_.delete())
    files = Seq.empty
  }
}
