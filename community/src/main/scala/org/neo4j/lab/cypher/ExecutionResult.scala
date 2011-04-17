package org.neo4j.lab.cypher

import org.neo4j.graphdb.Node

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 21:02 
 */

class ExecutionResult(from: Map[String, () => Seq[Node]],
                      projection: Projection,
                      filter: Map[String, (Node) => Boolean]) extends Iterator[ResultRow] {
  def hasNext = false

  def next() = null

  def columnNames: List[String] = from.keys.toList

  def column(name: String): Seq[Node] =
    from.get(name) match {
      case None => List()
      case Some(f) => f.apply().filter(getFilter(name))
    }


  private def getFilter(name: String): (Node) => Boolean =
    filter.get(name) match {
      case None => (n) => true
      case Some(filter) => filter
    }

}

class Projection

class ResultRow