package org.neo4j.lab.cypher.pipes

/**
 * Created by Andres Taylor
 * Date: 4/19/11
 * Time: 11:12 
 */

trait SelectionPipe extends Pipe {
  def source : Pipe
}
