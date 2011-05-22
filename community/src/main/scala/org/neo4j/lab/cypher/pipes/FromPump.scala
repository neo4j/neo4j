package org.neo4j.lab.cypher.pipes

import org.neo4j.graphdb.{PropertyContainer, Node}

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

class FromPump[T <: PropertyContainer](name: String, source: Iterable[T]) extends Pipe {
  def columnNames: List[String] = List(name)

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((x) => {
      val map = Map(name -> x)
      f.apply(map)
    })
  }
}