package org.neo4j.lab.cypher.pipes

import org.neo4j.graphdb.{Node, NotFoundException}

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 22:34 
 */

class PropertyOutputPipe(source: Pipe, varName: String, propName: String) extends Pipe {
  private val newColumn = varName + "." + propName

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((map) => {
      val node = map.getOrElse(varName, throw new NotFoundException()).asInstanceOf[Node]
      f.apply(map ++ Map(newColumn -> node.getProperty(propName)))
    })
  }
}