package org.neo4j.cypher.pipes

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb._
import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{PropertyIdentifier, Identifier}

class SortPipeTest {
  @Test def emptyInIsEmptyOut() {
    val inner = new StartPipe[Node]("x", List())
    val sortPipe = new SortPipe("x", inner)

    assertEquals(List(), sortPipe.toList)
  }

  @Test def simpleSortingIsSupported() {
    val inner = new FakePipe(List(PropertyIdentifier("x","x")), List(Map("x" -> "B"), Map("x" -> "A")))
    val sortPipe = new SortPipe("x", inner)

    assertEquals(List(Map("x" -> "A"), Map("x" -> "B")), sortPipe.toList)
  }
}

class FakePipe(identifiers: Seq[Identifier], data: Seq[Map[String, Any]]) extends Pipe {
  val symbols: SymbolTable = new SymbolTable(identifiers.map(x => x.name -> x).toMap)

  def foreach[U](f: (Map[String, Any]) => U) {
    data.foreach(f(_))
  }
}