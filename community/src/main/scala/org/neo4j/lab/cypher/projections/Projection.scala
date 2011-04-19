package org.neo4j.lab.cypher.projections

import org.neo4j.lab.cypher.pipes.Pipe
import org.neo4j.graphdb.{Node, NotFoundException}

/**
 * Created by Andres Taylor
 * Date: 4/19/11
 * Time: 19:18 
 */

class Projection(source: Pipe, transformers: Seq[(Map[String, Any]) => (Map[String, Any])]) extends Traversable[Map[String, Any]] {
  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((m) => {
      val result = transformers.map((transformer) => transformer.apply(m)).reduceLeft(_ ++ _)
      f.apply(result)
    })
  }

  def columnAs[T](column: String): Iterator[T] = {
    this.map((map) => {
      val item: Any = map.getOrElse(column, throw new NotFoundException())
      item.asInstanceOf[T]
    }).toIterator
  }
}