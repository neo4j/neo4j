package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction
import scala.Some

/**
 * Created by Andres Taylor
 * Date: 5/19/11
 * Time: 17:06 
 */

abstract class Pattern

object RelatedTo {
  def apply(left: String, right: String, relName: String, relType: String, direction: Direction) =
    new RelatedTo(left, right, Some(relName), Some(relType), direction)
}

case class RelatedTo(left: String, right: String, relName: Option[String], relType: Option[String], direction: Direction) extends Pattern
