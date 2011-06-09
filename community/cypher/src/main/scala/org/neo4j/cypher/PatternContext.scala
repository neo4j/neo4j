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

import commands.{RelationshipType, NodeType, RelatedTo, Match}
import org.neo4j.graphdb._
import org.neo4j.graphmatching._
import scala.collection.JavaConversions._
import java.lang.UnsupportedOperationException

class PatternContext(val symbolTable: SymbolTable) {

  val group = new PatternGroup
  val nodes = scala.collection.mutable.Map[String, PatternNode]()
  val rels = scala.collection.mutable.Map[String, PatternRelationship]()

  symbolTable.identifiers.foreach((kv) => kv match {
    case (name: String, symbolType: NodeType) => nodes(name) = getOrCreateNode(name)
    case (name: String, symbolType: RelationshipType) => rels(name) = getOrCreateRelationship(name)
  })

  def createPatterns(matching: Option[Match]) {
    matching match {
      case Some(m) => m.patterns.foreach((pattern) => {
        pattern match {
          case RelatedTo(left, right, relName, relationType, direction) => createRelationshipPattern(left, right, relationType, direction, relName)
        }
      })
      case None =>
    }
  }

  def createRelationshipPattern(left: String, right: String, relationType: Option[String], direction: Direction, relName: Option[String]) {
    val leftPattern = getOrCreateNode(left)
    val rightPattern = getOrCreateNode(right)
    val rel = relationType match {
      case Some(relType) => leftPattern.createRelationshipTo(rightPattern, DynamicRelationshipType.withName(relType), direction)
      case None => leftPattern.createRelationshipTo(rightPattern, direction)
    }

    relName match {
      case None =>
      case Some(name) => {
        addRelationship(name, rel)
        rel.setLabel(name)
      }
    }
  }

  def getOrCreateNode(name: String): PatternNode = {
    if (rels.contains(name)) {
      throw new SyntaxError("Variable \"" + name + "\" already defined as a relationship.")
    }

    nodes.getOrElse(name, {
      val pNode = new PatternNode(group, name)
      nodes(name) = pNode
      symbolTable.registerNode(name)
      pNode
    })
  }


  def checkConnectednessOfPatternGraph(startIdentifiers : List[String]) {
    val visited = scala.collection.mutable.HashSet[String]()

    def visit(visitedObject: PatternType) {
      val label = visitedObject.getLabel
      if (label == null || !visited.contains(label)) {
        if (label != null) {
          visited.add(label)
        }

        visitedObject match {
          case node: PatternNode => node.getAllRelationships.foreach(visit)
          case rel: PatternRelationship => {
            visit(rel.getFirstNode)
            visit(rel.getSecondNode)
          }
        }

      }
    }

    startIdentifiers.map((item) => patternObject(item)).foreach(_ match {
      case None => throw new SyntaxError("Encountered a part of the pattern that is not part of the pattern. If you see this, please report this problem!")
      case Some(obj) => visit(obj)
    })

    val notVisitedParts = identifiers -- visited
    if (notVisitedParts.nonEmpty) {
      throw new SyntaxError("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " +
        notVisitedParts.mkString("", ", ", ""))
    }

  }

  def getOrCreateRelationship(name: String): PatternRelationship = {
    symbolTable.registerRelationship(name)
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
    symbolTable.registerRelationship(name)
  }

  def getOrThrow(name: String): PatternType = nodes.get(name) match {
    case Some(x) => x.asInstanceOf[PatternType]
    case None => rels.get(name) match {
      case Some(x) => x.asInstanceOf[PatternType]
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

  type PatternType = AbstractPatternObject[_ <: PropertyContainer]

  def patternObject(key: String): Option[PatternType] = nodes.get(key) match {
    case Some(node) => Some(node.asInstanceOf[PatternType])
    case None => rels.get(key) match {
      case Some(rel) => Some(rel.asInstanceOf[PatternType])
      case None => None
    }
  }

  def bindStartPoint[U](startPoint: (String, Any)) {
    startPoint match {
      case (identifier: String, node: Node) => nodes(identifier).setAssociation(node)
      case (identifier: String, rel: Relationship) => rels(identifier).setAssociation(rel)
    }
  }

  def getPatternMatches(fromRow: Map[String, Any]): Iterable[Map[String,Any]] = {
    val startKey = fromRow.keys.head
    val startPNode = nodes(startKey)
    val startNode = fromRow(startKey).asInstanceOf[Node]
    val matches : Iterable[PatternMatch] = PatternMatcher.getMatcher.`match`(startPNode, startNode)
    matches.map(patternMatch => {
      (nodes.map {case(name:String,node:PatternNode) => name -> patternMatch.getNodeFor(node) } ++
      rels.map {case(name:String,rel:PatternRelationship) => name -> patternMatch.getRelationshipFor(rel)}).toMap[String,Any]
    })
  }

}