package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 22:14 
 */

class FilteringPipe(from: Pipe, filter: (Map[String, Any]) => Boolean) extends Pipe {
  def foreach[U](f: (Map[String, Any]) => U) {
    from.filter(filter).foreach((map) => f.apply(map))
  }
}
