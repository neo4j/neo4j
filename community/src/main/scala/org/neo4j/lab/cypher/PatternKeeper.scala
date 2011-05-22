package org.neo4j.lab.cypher

import org.neo4j.graphdb.PropertyContainer
import collection.immutable.Map
import org.neo4j.graphmatching.{PatternRelationship, AbstractPatternObject, PatternNode}
import scala.Some
import org.apache.commons.lang.NotImplementedException

/**
 * Created by Andres Taylor
 * Date: 5/20/11
 * Time: 13:54 
 */

class PatternKeeper {
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

  def getOrCreateRelationship(name: String): PatternRelationship = {
    throw new NotImplementedException("graph-matching doesn't support this yet. Revisit when it does.")
//     if (nodes.contains(name))
//       throw new SyntaxError(name + " already defined as a node")
//
//     rels.getOrElse(name, {
//       val pRel = new PatternRelationship(name)
//       rels(name) = pRel
//       pRel
//     })
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

  def nodesMap: Map[String, PatternNode] = nodes.toMap
  def relationshipsMap : Map[String, PatternRelationship] = rels.toMap

}