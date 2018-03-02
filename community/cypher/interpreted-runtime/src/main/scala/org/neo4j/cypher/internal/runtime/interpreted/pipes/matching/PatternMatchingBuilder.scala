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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.matching

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.Map

class PatternMatchingBuilder(patternGraph: PatternGraph,
                             predicates: Seq[Predicate],
                             variablesInClause: Set[String]) extends MatcherBuilder {
  def getMatches(sourceRow: ExecutionContext, state:QueryState): Traversable[ExecutionContext] = {
    val bindings: Map[String, AnyValue] =
      sourceRow.boundEntities(state.query.nodeOps.getById, state.query.relationshipOps.getById)
    val boundPairs: Map[String, Set[MatchingPair]] = extractBoundMatchingPairs(bindings)

    val undirectedBoundRelationships: Iterable[PatternRelationship] = bindings.keys.
      filter(z => patternGraph.contains(z)).
      filter(patternGraph(_).exists(_.isInstanceOf[PatternRelationship])).
      flatMap(patternGraph(_).asInstanceOf[Seq[PatternRelationship]]).
      filter(_.dir == SemanticDirection.BOTH)

    val mandatoryPattern: Traversable[ExecutionContext] = if (undirectedBoundRelationships.isEmpty) {
      createPatternMatcher(boundPairs, includeOptionals = false, sourceRow, state)
    } else {
      val boundRels = createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships, bindings)

      boundRels.
        flatMap(relMap => createPatternMatcher(relMap ++ boundPairs, includeOptionals = false, sourceRow, state))
    }

    mandatoryPattern
  }

  private def createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships:
                                                             Iterable[PatternRelationship], bindings: Map[String,
    Any]): Seq[Map[String, Set[MatchingPair]]] = {
    val toList = undirectedBoundRelationships.map(patternRel => {
      val rel = bindings(patternRel.key).asInstanceOf[RelationshipValue]
      val x = patternRel.key -> Set(MatchingPair(patternRel, rel))

      // Outputs the first direction of the pattern relationship
      val a1 = patternRel.startNode.key -> Set(MatchingPair(patternRel.startNode, rel.startNode()))
      val a2 = patternRel.endNode.key -> Set(MatchingPair(patternRel.endNode, rel.endNode()))

      // Outputs the second direction of the pattern relationship
      val b1 = patternRel.startNode.key -> Set(MatchingPair(patternRel.startNode, rel.endNode()))
      val b2 = patternRel.endNode.key -> Set(MatchingPair(patternRel.endNode, rel.startNode()))

      Seq(Map(x, a1, a2), Map(x, b1, b2))
    }).toList
    cartesian(toList).map(_.reduceLeft(_ ++ _))
  }

  // This method takes  a Seq of Seq and produces the cartesian product of all inner Seqs
  // I'm committing this code, but it's all Tobias' doing.
  private def cartesian[T](lst: Seq[Seq[T]]): Seq[Seq[T]] =
    lst.foldRight(List(List[T]()))(// <- the type T needs to be specified here
      (element: Seq[T], result: List[List[T]]) => // types for better readability
        result.flatMap(r => element.map(e => e :: r))
    ).toIndexedSeq

  private def createPatternMatcher(boundPairs: Map[String, Set[MatchingPair]], includeOptionals: Boolean, source: ExecutionContext, state:QueryState): Traversable[ExecutionContext] =
      new PatternMatcher(boundPairs, predicates, source, state, variablesInClause)


  private def extractBoundMatchingPairs(bindings: Map[String, Any]): Map[String, Set[MatchingPair]] = bindings.flatMap {
    case (key, node: NodeValue) if patternGraph.contains(key) =>
      Seq(key -> patternGraph(key).map(pNode => MatchingPair(pNode, node)).toSet)
    case (key, rel: RelationshipValue) if patternGraph.contains(key) =>
      val patternRels = patternGraph(key).asInstanceOf[Seq[PatternRelationship]]
      patternRels.flatMap(pRel => {
        def extractMatchingPairs(startNode: PatternNode, endNode: PatternNode): Seq[(String, Set[MatchingPair])] = {
          val t1 = startNode.key -> Set(MatchingPair(startNode, rel.startNode()))
          val t2 = endNode.key -> Set(MatchingPair(endNode, rel.endNode()))
          val t3 = pRel.key -> Set(MatchingPair(pRel, rel))

          // Check that found end nodes correspond to what is already in scope
          if (bindings.get(t1._1).forall(_ == t1._2.head.entity) &&
              bindings.get(t2._1).forall(_ == t2._2.head.entity))
            Seq(t1, t2, t3)
          else
            Seq.empty[(String, Set[MatchingPair])]
        }

        pRel.dir match {
          case SemanticDirection.OUTGOING => extractMatchingPairs(pRel.startNode, pRel.endNode)
          case SemanticDirection.INCOMING => extractMatchingPairs(pRel.endNode, pRel.startNode)
          case SemanticDirection.BOTH if bindings.contains(pRel.key) => Seq(pRel.key -> Set(MatchingPair(pRel, rel)))
          case SemanticDirection.BOTH => Seq.empty[(String, Set[MatchingPair])]
        }
      })
    case (key, _) => Seq(key -> Set.empty[MatchingPair])
  }

  def name = "PatternMatcher"

  override def startPoint: String = ""
}

