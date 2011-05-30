/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.sunshine

import org.neo4j.graphdb.PropertyContainer
import collection.immutable.Map
import org.neo4j.graphmatching.{PatternRelationship, AbstractPatternObject, PatternNode}
import org.apache.commons.lang.NotImplementedException
import scala.Some

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
       throw new SyntaxError("Variable \"" + name + "\" already defined as a relationship.")

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
      throw new SyntaxError("Variable \"" + name + "\" already defined as a node.")

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

  def assertHas(variable:String) { if (!(nodes.contains(variable) || rels.contains(variable))) throw new SyntaxError("Unknown variable \""+ variable +"\".") }

  def variables = nodes.keySet ++ rels.keySet

  def patternObject(key:String):Option[AbstractPatternObject[_ <: PropertyContainer]] = nodes.get(key) match {
    case Some(node) => Some(node.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]])
    case None => rels.get(key) match {
      case Some(rel) => Some(rel.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]])
      case None => None
    }
  }

}