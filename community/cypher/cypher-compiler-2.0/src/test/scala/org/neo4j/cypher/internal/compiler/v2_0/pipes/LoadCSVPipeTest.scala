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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.junit.{Ignore, Test}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import java.net.URL
import java.io.PrintWriter
import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.commons.CypherJUnitSuite

class LoadCSVPipeTest extends CypherJUnitSuite with CreateTempFileTestSupport {

  @Test
  def should_handle_strings() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))

    val url = createFileURL { writer =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
    }
    val pipe = new LoadCSVPipe(input, NoHeaders, url, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result === List(
      Map("foo" -> Seq("1")),
      Map("foo" -> Seq("2")),
      Map("foo" -> Seq("3")),
      Map("foo" -> Seq("4"))
    ))
  }

  @Test
  def should_handle_with_headers() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))

    val url = createFileURL { writer =>
      writer.println("a,b")
      writer.println("1,2")
      writer.println("3,4")
    }
    val pipe = new LoadCSVPipe(input, HasHeaders, url, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result === List(
      Map("foo" -> Map("a" -> "1", "b" -> "2")),
      Map("foo" -> Map("a" -> "3", "b" -> "4"))
    ))
  }

  @Test
  def should_handle_with_headers_even_for_uneven_files() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))

    val url = createFileURL {
      writer =>
        writer.println("a,b")
        writer.println("1,2")
        writer.println("3")
    }
    val pipe = new LoadCSVPipe(input, HasHeaders, url, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result === List(
      Map("foo" -> Map("a" -> "1", "b" -> "2")),
      Map("foo" -> Map("a" -> "3"))
    ))
  }

  @Test
  def should_give_a_helpful_message_when_asking_for_headers_with_empty_file() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))

    val url = createFileURL(_ => {})
    val pipe = new LoadCSVPipe(input, HasHeaders, url, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    assert(result === List())
  }

  @Test
  def should_be_able_to_read_a_file_twice() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx, ctx))

    val url = createFileURL { writer =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
    }
    val pipe = new LoadCSVPipe(input, NoHeaders, url, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result.size === 8)
  }

  @Ignore("2014-01-30 Andres & Jakub - Tries to get data from internet")
  @Test
  def should_be_able_to_handle_an_http_file() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))
    val pipe = new LoadCSVPipe(input, NoHeaders, new URL("https://dl.dropboxusercontent.com/u/47132/test.csv"), "name")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result === List(
      Map("name" -> Seq("Stefan")),
      Map("name" -> Seq("Jakub")),
      Map("name" -> Seq("Andres")),
      Map("name" -> Seq("Davide")),
      Map("name" -> Seq("Chris"))
    ))
  }

  private def createFileURL(f: PrintWriter => Unit) = new URL(s"file://${createTempFile("cypher", ".csv", f)}")
}
