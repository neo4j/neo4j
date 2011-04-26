package org.neo4j.lab.cypher

/**
 * Created by Andres Taylor
 * Date: 4/26/11
 * Time: 08:49 
 */

import org.junit.Assert._

import org.junit.Test
import pipes.Pipe


class CurrentRowTest {
  @Test def shouldReturnAPipeWithUnmetDependencies() {
    val row = new CurrentRow

    val pipe = new FakePipe(columnNames = List(), dependsOn = List("foo"))

    val head = row.addPipe(pipe).head

    assertEquals(pipe, head)
  }

  @Test def shouldAcceptAPipeWithNoDependencies() {
    val row = new CurrentRow

    val pipe = new FakePipe(columnNames = List(), dependsOn = List())

    val result = row.addPipe(pipe)


    assertTrue(result.isEmpty)
  }

  @Test def shouldConnectPipes() {
    val row = new CurrentRow

    val pipeA = new FakePipe(columnNames = List("A"), dependsOn = List())
    val pipeB = new FakePipe(columnNames = List("A"), dependsOn = List("A") )

    row.addPipe(pipeA)
    row.addPipe(pipeB)

    assertEquals(pipeB.input.get, pipeA)
  }


  class FakePipe(val columnNames: List[String], val dependsOn: List[String]) extends Pipe {
    def foreach[U](f: (Map[String, Any]) => U) {}
  }

}