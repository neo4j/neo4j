/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_2.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_2.commands.{Pattern, RelatedTo, VarLengthRelatedTo}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.matching.{PatternGraph, PatternNode, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

import scala.collection.mutable

trait PatternGraphBuilder {
  def buildPatternGraph(symbols: SymbolTable, patterns: Seq[Pattern]): PatternGraph = {

    if(patterns.isEmpty)
      return new PatternGraph(Map.empty, Map.empty, Seq.empty, Seq.empty)

    val patternNodeMap: mutable.Map[String, PatternNode] = scala.collection.mutable.Map()
    val patternRelMap: mutable.Map[String, PatternRelationship] = scala.collection.mutable.Map()

    def takeOnPattern(x: Pattern): Boolean = {
      x match {
        case r: RelatedTo          => takeOnRelatedTo(r)
        case r: VarLengthRelatedTo => takeOnVarLengthRel(r)
        case _                     => false
      }
    }

    def takeOnRelatedTo(r: RelatedTo) = {
      val left = r.left
      val right = r.right
      val relName = r.relName
      val leftNode: PatternNode = patternNodeMap.getOrElseUpdate(left.name, new PatternNode(left))
      val rightNode: PatternNode = patternNodeMap.getOrElseUpdate(right.name, new PatternNode(right))
      patternRelMap(relName) = leftNode.relateTo(relName, rightNode, r)
      true
    }

    def takeOnVarLengthRel(r: VarLengthRelatedTo) = {
      val startNode: PatternNode = patternNodeMap.getOrElseUpdate(r.left.name, new PatternNode(r.left))
      val endNode: PatternNode = patternNodeMap.getOrElseUpdate(r.right.name, new PatternNode(r.right))
      patternRelMap(r.pathName) = startNode.relateViaVariableLengthPathTo(r.pathName, endNode, r.minHops, r.maxHops, r.relTypes, r.direction, r.relIterator, r.properties)
      true
    }

    // Start from a pattern that is connected to something already bound
    val s = patterns.find(pattern => pattern.possibleStartPoints.exists(tuple => symbols.hasIdentifierNamed(tuple._1)))


    val startPoint = s.getOrElse(throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + patterns.flatMap(_.possibleStartPoints.map(_._1)).distinct.sorted.mkString(", ")))

    val patternsLeft = mutable.Set[Pattern](patterns: _*)
    val boundPoints = mutable.Set[String](startPoint.possibleStartPoints.map(_._1): _*)
    patternsLeft -= startPoint
    takeOnPattern(startPoint)

    // Now we loop until we don't take on more patterns
    var continue = true
    while (continue) {
      val n = patternsLeft.find(pattern => pattern.possibleStartPoints.exists(tuple => boundPoints(tuple._1)))

      if (n.isEmpty)
        continue = false
      else {
        val nextPattern = n.get
        nextPattern.possibleStartPoints.foreach {
          case (key, _) => boundPoints += key
        }
        takeOnPattern(nextPattern)
        patternsLeft -= nextPattern
      }
    }

    val patternsDone = (patterns.toSet -- patternsLeft).toSeq

    new PatternGraph(patternNodeMap.toMap, patternRelMap.toMap, symbols.keys, patternsDone)
  }
}
