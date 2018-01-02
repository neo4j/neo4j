/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import pipes.QueryState
import org.neo4j.graphdb.{Relationship, Node}
import collection.Map

class PatternMatcher(bindings: Map[String, Set[MatchingPair]],
                     predicates: Seq[Predicate],
                     source:ExecutionContext,
                     state:QueryState,
                     identifiersInClause: Set[String])
  extends Traversable[ExecutionContext] {
  val boundNodes: Map[String, Set[MatchingPair]] =
    bindings.filter(x => x._2.nonEmpty && x._2.head.patternElement.isInstanceOf[PatternNode])
  val boundRels: Map[String, Set[MatchingPair]] =
    bindings.filter(x => x._2.nonEmpty && x._2.head.patternElement.isInstanceOf[PatternRelationship])

  def foreach[U](f: ExecutionContext => U) {
    debug("startPatternMatching")

    traverseNode(boundNodes.values.flatten.toSet, createInitialHistory, f)
  }


  def createInitialHistory: InitialHistory = {

    val relationshipsInContextButNotInPattern = source.collect {
      case (key, r: Relationship) if !boundRels.contains(key) && identifiersInClause.contains(key) => r
    }.toSeq

    new InitialHistory(source, relationshipsInContextButNotInPattern)
  }

  protected def traverseNextSpecificNode[U](remaining: Set[MatchingPair],
                                            history: History,
                                            yielder: ExecutionContext => U,
                                            current: MatchingPair,
                                            alreadyInExtraWork: Boolean): Boolean = {
    debug(current, history, remaining)

    if (!current.matchesBoundEntity(boundNodes)) {
      debug("Didn't match bound node")
      return false
    }

    val newHistory = history.add(current)
    if (!isMatchSoFar(newHistory)) {
      debug("failed subgraph because of predicate")
      return false
    }

    val notYetVisited: List[PatternRelationship] = getPatternRelationshipsNotYetVisited(current.patternNode, history)

    notYetVisited match {
      case List()       => traverseNextNodeOrYield(remaining - current, newHistory, yielder)
      case List(single) => traverseRelationship(current, single, newHistory, remaining - current, yielder)
      case _            => traverseRelationship(current, notYetVisited.head, newHistory, remaining, yielder)
    }
  }

  private def traverseNode[U](remaining: Set[MatchingPair],
                              history: History,
                              yielder: ExecutionContext => U): Boolean = {

    val current: MatchingPair = remaining.head

    val currentNodeId = current.entity.asInstanceOf[Node].getId

    current.patternNode.canUseThis(currentNodeId, state, history.toMap) &&
      traverseNextSpecificNode(remaining, history, yielder, current, alreadyInExtraWork = false)
  }

  private def traverseNextNodeFromRelationship[U](rel: GraphRelationship,
                                                  gNode: Node,
                                                  nextPNode: PatternNode,
                                                  currentRel: PatternRelationship,
                                                  history: History,
                                                  remaining: Set[MatchingPair],
                                                  yielder: ExecutionContext => U): Boolean = {
    debug(rel, gNode, nextPNode, currentRel, history, remaining)
    val current = MatchingPair(currentRel, rel)

    val boundEntity = current.matchesBoundEntity(boundRels)
    if (!boundEntity) {
      debug("Didn't match bound relationship")
      false
    } else {

      val newHistory = history.add(current)

      if (isMatchSoFar(newHistory)) {
        val nextNode = rel.getOtherNode(gNode)

        val nextPair = MatchingPair(nextPNode, nextNode)

        remaining.find(_.patternElement.key == nextPNode.key) match {
          case None => traverseNode(remaining ++ Set(nextPair), newHistory, yielder)
          case Some(x) => if (x.entity == nextNode)
            traverseNode(remaining ++ Set(nextPair), newHistory, yielder)
          else {
            debug("other side of relationship already found, and doesn't match")
            false
          }
        }


      }
      else {
        debug("failed because of a predicate")
        false
      }
    }

  }

  private def alreadyPinned[U](currentRel: PatternRelationship, x: GraphRelationship): Boolean = {
    boundRels.get(currentRel.key) match {
      case Some(pinnedRel) => pinnedRel.forall(_.matches(x))
      case None => true
    }
  }

  private def traverseRelationship[U](currentNode: MatchingPair,
                                      currentRel: PatternRelationship,
                                      history: History,
                                      remaining: Set[MatchingPair],
                                      yielder: ExecutionContext => U): Boolean = {
    debug(currentNode, currentRel, history, remaining)

    val (pNode, gNode) = currentNode.getPatternAndGraphPoint

    val relationships = currentNode.getGraphRelationships(currentRel, state, history.toMap)

    val notVisitedRelationships: Seq[GraphRelationship] = history.
      removeSeen(relationships).
      filter(x => alreadyPinned(currentRel, x))

    val nextPNode = currentRel.getOtherNode(pNode)

    /*
     We need to know if any of these sub-calls results in a yield. If none do, and we're
     looking at an optional pattern relationship, we'll output a null as match.
    */
    val yielded = notVisitedRelationships.map(rel => traverseNextNodeFromRelationship(rel, gNode, nextPNode, currentRel, history, remaining, yielder)).foldLeft(false)(_ || _)

    if (yielded) {
      return true
    }

    debug("failed to find matching relationship")
    false
  }

  private def isMatchSoFar(history: History): Boolean = {
    val m = history.toMap
    val predicate = predicates.filter(predicate=> !predicate.containsIsNull && predicate.symbolTableDependencies.forall(m contains))
    predicate.forall(_.isTrue(m)(state))
  }

  private def traverseNextNodeOrYield[U](remaining: Set[MatchingPair], history: History, yielder: ExecutionContext => U): Boolean = {
    debug(history, remaining)

    if (remaining.isEmpty) {
      yieldThis(yielder, history)
    } else {
      traverseNode(remaining, history, yielder)
    }
  }

  private def yieldThis[U](yielder: ExecutionContext => U, history: History): Boolean = {
    val toMap = history.toMap
    debug(history, toMap)

    yielder(toMap)
    true
  }

  private def getPatternRelationshipsNotYetVisited[U](patternNode: PatternNode, history: History): List[PatternRelationship] =
    history.removeSeen(patternNode.relationships).toList

  protected val isDebugging = false

  private def debug[U](history: History, remaining: Set[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseNextNodeOrYield
      history=%s
      remaining=%s)
      """, history, remaining.toList))
  }

  private def debug[U](current: MatchingPair, history: History, remaining: Set[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseNode
    current=%s
    history=%s
    remaining=%s
    """, current, history, remaining.toList))
  }

  private def debug[U](current: MatchingPair, pRel: PatternRelationship, history: History, remaining: Set[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseRelationship
    current=%s
    pRel=%s
    history=%s
    remaining=%s
    """, current, pRel, history, remaining.toList))
  }

  private def debug[U](history: History, resultMap: Map[String, Any]) {
    if (isDebugging)
      println(String.format("""yield(history=%s) => %s
    """, history, resultMap))
  }

  private def debug(rel: GraphRelationship, node: Node, pNode: PatternNode, pRel: PatternRelationship, history: History, remaining: Set[MatchingPair]) {
    if (isDebugging)
      println(String.format("""traverseNextNodeFromRelationship
    rel=%s
    node=%s
    pNode=%s
    pRel=%s
    history=%s
    remaining=%s
    """, rel, node, pNode, pRel, history, remaining))
  }


  def debug(message: String) {
    if (isDebugging) println(message)
  }

}
