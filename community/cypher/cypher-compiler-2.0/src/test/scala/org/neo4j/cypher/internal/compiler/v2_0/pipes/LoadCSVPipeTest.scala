package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.scalatest.Assertions
import org.junit.{After, Test}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import java.io.PrintWriter
import scala.reflect.io.File

class LoadCSVPipeTest extends Assertions {

  @Test
  def should_handle_longs() {
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
