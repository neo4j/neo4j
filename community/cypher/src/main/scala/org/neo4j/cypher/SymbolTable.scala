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
package org.neo4j.cypher

import commands.{Start, RelationshipStartItem, NodeStartItem}
import org.neo4j.graphdb.PropertyContainer
import collection.immutable.Map
import scala.Some
import org.neo4j.graphmatching.{PatternGroup, PatternRelationship, AbstractPatternObject, PatternNode}
import scala.collection.JavaConverters._
import java.lang.UnsupportedOperationException

class SymbolTable {
  val group = new PatternGroup
  val nodes = scala.collection.mutable.Map[String, PatternNode]()
  val rels = scala.collection.mutable.Map[String, PatternRelationship]()

  def getOrCreateNode(name: String): PatternNode = {
    if (rels.contains(name)) {
      throw new SyntaxError("Variable \"" + name + "\" already defined as a relationship.")
    }

    nodes.getOrElse(name, {
      val pNode = new PatternNode(group, name)
      nodes(name) = pNode
      pNode
    })
  }


  def checkConnectednessOfPatternGraph(start: Start) {
    val visited = scala.collection.mutable.HashSet[String]()

    def visit(visitedObject: AbstractPatternObject[_ <: PropertyContainer]) {
      val label = visitedObject.getLabel
      if (label == null || !visited.contains(label)) {
        if (label != null) {
          visited.add(label)
        }

        visitedObject match {
          case node: PatternNode => node.getAllRelationships.asScala.foreach(visit)
          case rel: PatternRelationship => {
            visit(rel.getFirstNode)
            visit(rel.getSecondNode)
          }
        }

      }
    }

    start.startItems.map((item) => patternObject(item.variable)).foreach(_ match {
      case None => throw new SyntaxError("Encountered a part of the pattern that is not part of the pattern. If you see this, please report this problem!")
      case Some(obj) => visit(obj)
    })

    val notVisitedParts = identifiers -- visited
    if (notVisitedParts.nonEmpty) {
      throw new SyntaxError("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These variables were found to be disconnected: " +
        notVisitedParts.mkString("", ", ", ""))
    }

  }

  def addStartItems(start: Start) {
    start.startItems.foreach((item) => {
      item match {
        case relItem: RelationshipStartItem => getOrCreateRelationship(item.variable)
        case nodeItem: NodeStartItem => getOrCreateNode(item.variable)
      }
    })
  }


  def getOrCreateRelationship(name: String): PatternRelationship = {
    throw new UnsupportedOperationException("graph-matching doesn't support this yet. Revisit when it does.")
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
    if (nodes.contains(name)) {
      throw new SyntaxError("Variable \"" + name + "\" already defined as a node.")
    }

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

  def relationshipsMap: Map[String, PatternRelationship] = rels.toMap

  def assertHas(variable: String) {
    if (!(nodes.contains(variable) || rels.contains(variable))) {
      throw new SyntaxError("Unknown variable \"" + variable + "\".")
    }
  }

  def identifiers = nodes.keySet ++ rels.keySet

  def patternObject(key: String): Option[AbstractPatternObject[_ <: PropertyContainer]] = nodes.get(key) match {
    case Some(node) => Some(node.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]])
    case None => rels.get(key) match {
      case Some(rel) => Some(rel.asInstanceOf[AbstractPatternObject[_ <: PropertyContainer]])
      case None => None
    }
  }

}