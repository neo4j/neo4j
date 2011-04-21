package org.neo4j.lab.cypher.pipes

import org.neo4j.graphdb.Node

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

class FromPump(name: String, source: Iterable[Node]) extends Pipe {
  def columnNames: List[String] = List(name)
  def dependsOn = List[String]()
  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((x) => {
      val map = Map(name -> x)
      f.apply(map)
    })
  }

  override def setInput(input: Pipe) {
    throw new RuntimeException("FromPumps have no input, silly...")
  }
}