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
package org.neo4j.cypher.pipes.matching

import org.neo4j.cypher.commands.Clause

class PatternMatcher(startPoint: PatternNode, bindings: Map[String, MatchingPair], clauses:Seq[Clause]) extends Traversable[Map[String, Any]] {

  def foreach[U](f: (Map[String, Any]) => U) {
    traverseNode(MatchingPair(startPoint, startPoint.pinnedEntity.get), History(), bindings.values.toSeq, f)
  }

  private def traverseNode[U](current: MatchingPair,
                              history: History,
                              remaining: Seq[MatchingPair],
                              yielder: Map[String, Any] => U): Boolean = {
    debug(current, history, remaining)

    val (pNode, gNode) = current.getPatternAndGraphPoint

    bindings.get(pNode.key) match {
      case Some(pinnedNode) => if (pinnedNode.entity != gNode) return false
      case None =>
    }

    val newHistory = history.add(current)
    if(!isMatchSoFar(newHistory))
      return false

    val notYetVisited = getPatternRelationshipsNotYetVisited(pNode, history)

    if (notYetVisited.isEmpty) {
      traverseNextNodeOrYield(remaining, history.add(current), yielder)
    } else {
      /*
      We only care about the first pattern relationship. We'll add this current position
      in future, so that remaining pattern relationships can be traversed.
      */

      traverseRelationship(current, notYetVisited.head, newHistory, remaining ++ Seq(current), yielder)
    }
  }

  private def traverseRelationship[U](currentNode: MatchingPair,
                                      currentRel: PatternRelationship,
                                      history: History,
                                      remaining: Seq[MatchingPair],
                                      yielder: (Map[String, Any]) => U): Boolean = {
    debug(currentNode, currentRel, history, remaining)

    val (pNode, gNode) = currentNode.getPatternAndGraphPoint

    val notVisitedRelationships = history.filter(currentNode.getGraphRelationships(currentRel))

    val nextPNode = currentRel.getOtherNode(pNode)

    /*
    We need to know if any of these sub-calls results in a yield. If none do, and we're
    looking at an optional pattern relationship, we'll output a null as match.
     */
    val yielded = notVisitedRelationships.map(rel => {
      val nextNode = rel.getOtherNode(gNode)
      val newHistory = history.add(MatchingPair(currentRel, rel))

      if(isMatchSoFar(newHistory))
        traverseNode(MatchingPair(nextPNode, nextNode), newHistory, remaining, yielder)
      else
        false

    }).foldLeft(false)(_ || _)

    if(yielded) {
      return true
    }

    if (currentRel.optional) {
      return traverseNextNodeOrYield(remaining, history.add(currentNode).add(MatchingPair(currentRel, null)), yielder)
    }

    false
  }

  private def isMatchSoFar(history:History):Boolean = {
    val m = history.toMap
    val validClause = clauses.filter(_.dependsOn.forall(m.contains))
    validClause.forall( _.isMatch(m) )
  }

  private def traverseNextNodeOrYield[U](remaining: Seq[MatchingPair], history: History, yielder: Map[String, Any] => U): Boolean = {
    debug(history, remaining)

    if (remaining.isEmpty) {
      yieldThis(yielder, history)
      true
    }
    else {
      traverseNode(remaining.head, history, remaining.tail, yielder)
    }
  }

  private def yieldThis[U](yielder: Map[String, Any] => U, history: History) {
    debug(history, history.toMap)

    yielder(history.toMap)
  }

  private def getPatternRelationshipsNotYetVisited[U](patternNode: PatternNode, history: History): List[PatternRelationship] = history.filter(patternNode.relationships.toSet).toList

  val isDebugging = false

  def debug[U](history: History, remaining: Seq[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseNextNodeOrYield
      history=%s
      remaining=%s)""", history, remaining))
  }

  def debug[U](current: MatchingPair, history: History, remaining: Seq[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseNode
    current=%s
    history=%s
    remaining=%s
    """, current, history, remaining))
  }

  def debug[U](current: MatchingPair, pRel: PatternRelationship, history: History, remaining: Seq[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseRelationship
    current=%s
    pRel=%s
    history=%s
    remaining=%s
    """, current, pRel, history, remaining))
  }

  def debug[U](history: History, resultMap: Map[String, Any]) {
    if (isDebugging)
      println(String.format("""yield(history=%s) => %s
    """, history, resultMap))
  }

}