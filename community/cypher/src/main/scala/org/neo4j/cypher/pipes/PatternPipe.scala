package org.neo4j.cypher.pipes

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

import collection.immutable.Map
import org.neo4j.cypher.{SymbolTable, PatternContext}
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.graphmatching.{PatternRelationship, PatternNode, PatternMatcher}
import scala.collection.JavaConversions._
import org.neo4j.cypher.commands.Pattern

class PatternPipe(source: Pipe, patterns: Seq[Pattern]) extends Pipe {

  val patternContext: PatternContext = new PatternContext(source, patterns)

  val symbols: SymbolTable = patternContext.symbolTable

  def foreach[U](f: Map[String, Any] => U) {
    patternContext.validatePattern(source.symbols)

    source.foreach((row) => {
      row.foreach(bindStartPoint(_))

      getPatternMatches(row).foreach(f)
    })
  }

  def bindStartPoint(startPoint: (String, Any)) {
    startPoint match {
      case (identifier: String, node: Node) => patternContext.nodes(identifier).setAssociation(node)
      case (identifier: String, rel: Relationship) => patternContext.rels(identifier).setAssociation(rel)
    }
  }

  def getPatternMatches(fromRow: Map[String, Any]): Iterable[Map[String, Any]] = {
    val startKey = fromRow.keys.head
    val startPNode = patternContext.nodes(startKey)
    val startNode = fromRow(startKey).asInstanceOf[Node]
    val matches = PatternMatcher.getMatcher.`match`(startPNode, startNode)
    matches.map(patternMatch => {
      val nodesMap = patternContext.nodes.map {
        case (name: String, node: PatternNode) => name -> patternMatch.getNodeFor(node)
      }

      val relsMap = patternContext.rels.map {
        case (name: String, rel: PatternRelationship) => name -> patternMatch.getRelationshipFor(rel)
      }

      (nodesMap ++ relsMap).toMap[String, Any]
    })
  }

}