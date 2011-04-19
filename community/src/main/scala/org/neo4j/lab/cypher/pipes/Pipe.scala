package org.neo4j.lab.cypher.pipes

import org.neo4j.graphdb.NotFoundException

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

trait Pipe extends Traversable[Map[String, Any]] {
  def join(other: Pipe): Pipe = new JoinPipe(this, other)



}