package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.scalatest.Assertions
import org.junit.{Ignore, After, Test}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import java.io.PrintWriter
import scala.reflect.io.File

class LoadCSVPipeTest extends Assertions {

  @Test
  def should_handle_strings() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))

    val fileName = createFile { writer =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
    }
    val pipe = new LoadCSVPipe(input, false, "file://" + fileName, "foo")

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
  def should_be_able_to_read_a_file_twice() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx, ctx))

    val fileName = createFile { writer =>
      writer.println("1")
      writer.println("2")
      writer.println("3")
      writer.println("4")
    }
    val pipe = new LoadCSVPipe(input, false, "file://" + fileName, "foo")

    //when
    val result = pipe.createResults(QueryStateHelper.empty).toList

    //then
    assert(result.size === 8)
  }

  @Ignore("2014-01-30 Andres & Jakub - Tries to get data from internet") @Test
  def should_be_able_to_handle_an_http_file() {
    //given
    val ctx = ExecutionContext.empty
    val input = new FakePipe(Iterator(ctx))
    val pipe = new LoadCSVPipe(input, false, "https://dl.dropboxusercontent.com/u/47132/test.csv", "name")

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
    files = Seq.empty
  }

}
