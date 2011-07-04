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

import commands._
import org.neo4j.graphmatching._
import pipes.Pipe
import scala.collection.JavaConversions._
import java.lang.UnsupportedOperationException
import collection.Seq
import org.neo4j.graphdb.{PropertyContainer, DynamicRelationshipType, Direction}

class PatternContext(source: Pipe, matching: Match) {

  val patternSymbolTypes: Seq[Identifier] = matching.patterns.map(pattern => createSymbolType(pattern)).flatten

  val symbolTable = source.symbols.add(patternSymbolTypes)

  val group = new PatternGroup
  val nodes = scala.collection.mutable.Map[String, PatternNode]()
  val rels = scala.collection.mutable.Map[String, PatternRelationship]()

  createStartItemsPatterns()
  createPatterns()

  private def createStartItemsPatterns() {
    source.symbols.identifiers.foreach( identifier => identifier match {
      case NodeIdentifier(identifier.name) => getOrCreateNode(identifier.name)
      case RelationshipIdentifier(identifier.name) => getOrCreateRelationship(identifier.name)
    })
  }


  private def createPatterns() {
    matching.patterns.foreach((pattern) => {
      pattern match {
        case RelatedTo(left, right, relName, relationType, direction) => createRelationshipPattern(left, right, relationType, direction, relName)
      }
    })
  }

  private def createSymbolType(pattern: Pattern): List[Identifier] = pattern match {
    case RelatedTo(left, right, relName, relType, direction) => List(Some(NodeIdentifier(left)), Some(NodeIdentifier(right)), relName match {
      case None => None
      case Some(name) => Some(RelationshipIdentifier(name))
    }).flatMap(_.toList)
  }

  private def createRelationshipPattern(left: String, right: String, relationType: Option[String], direction: Direction, relName: Option[String]) {
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

  private def getOrCreateNode(name: String): PatternNode = {
    if (rels.contains(name)) {
      throw new SyntaxException("Identifier \"" + name + "\" already defined as a relationship.")
    }

    nodes.getOrElse(name, {
      val pNode = new PatternNode(group, name)
      nodes(name) = pNode
      pNode
    })
  }


  def validatePattern(startIdentifiers: SymbolTable) {
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

    startIdentifiers.identifiers.map((item) => patternObject(item.name)).foreach(_ match {
      case None => throw new SyntaxException("Encountered a part of the pattern that is not part of the pattern. If you see this, please report this problem!")
      case Some(obj) => visit(obj)
    })

    val notVisitedParts = identifiers -- visited
    if (notVisitedParts.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " +
        notVisitedParts.mkString("", ", ", ""))
    }

  }

  private def getOrCreateRelationship(name: String): PatternRelationship = {
    throw new UnsupportedOperationException("graph-matching doesn't support this yet. Revisit when it does.")
    //     if (nodes.contains(name))
    //       throw new SyntaxException(name + " already defined as a node")
    //
    //     rels.getOrElse(name, {
    //       val pRel = new PatternRelationship(name)
    //       rels(name) = pRel
    //       pRel
    //     })
  }

  private def addRelationship(name: String, rel: PatternRelationship) {
    if (nodes.contains(name)) {
      throw new SyntaxException("Identifier \"" + name + "\" already defined as a node.")
    }

    rels(name) = rel
  }

  private def identifiers = nodes.keySet ++ rels.keySet

  private type PatternType = AbstractPatternObject[_ <: PropertyContainer]

  private def patternObject(key: String): Option[PatternType] = nodes.get(key) match {
    case Some(node) => Some(node.asInstanceOf[PatternType])
    case None => rels.get(key) match {
      case Some(rel) => Some(rel.asInstanceOf[PatternType])
      case None => None
    }
  }
}