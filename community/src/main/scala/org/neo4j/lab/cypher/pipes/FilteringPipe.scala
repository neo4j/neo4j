package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 22:14 
 */

class FilteringPipe(val dependsOn: List[String], filter: (Map[String, Any]) => Boolean) extends Pipe {
  def columnNames: List[String] = getInput.columnNames

  def foreach[U](f: (Map[String, Any]) => U) {
    getInput.filter(filter).foreach((map) => f.apply(map))
  }
}
