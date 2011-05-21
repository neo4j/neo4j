package org.neo4j.lab.cypher.commands

import org.neo4j.graphdb.Direction

/**
 * Created by Andres Taylor
 * Date: 5/19/11
 * Time: 17:06 
 */

abstract class Pattern

case class RelatedTo(left:String, right:String, relName:Option[String], relType:Option[String], direction:Direction) extends Pattern
