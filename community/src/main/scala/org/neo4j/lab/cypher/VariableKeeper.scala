package org.neo4j.lab.cypher

import org.neo4j.graphdb.PropertyContainer
import collection.immutable.Map
import org.neo4j.graphmatching.{PatternRelationship, AbstractPatternObject, PatternNode}
import scala.Some

/**
 * Created by Andres Taylor
 * Date: 5/20/11
 * Time: 13:54 
 */

class VariableKeeper {
  val nodes = scala.collection.mutable.Map[String, PatternNode]()
  val rels = scala.collection.mutable.Map[String, PatternRelationship]()

  def getOrCreateNode(name: String): PatternNode = {
    if (rels.contains(name))
      throw new SyntaxError(name + " already defined as a relationship")

    nodes.getOrElse(name, {
      val pNode = new PatternNode(name)
      nodes(name) = pNode
      pNode
    })
  }

  def addRelationship(name: String, rel: PatternRelationship) {
    if (nodes.contains(name))
      throw new SyntaxError(name + " already defined as a node")

    rels(name) = rel
  }


  def getOrThrow(name: String): AbstractPatternObject[_ <: PropertyContainer] = nodes.get(name) match {
    case Some(x) => x.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]]
    case None => rels.get(name) match {
      case Some(x) => x.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]]
      case None => throw new SyntaxError("No variable named " + name + " has been defined")
    }
  }

  def toMap: Map[String, PatternNode] = nodes.toMap

}