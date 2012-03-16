/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb.{Relationship, Node, Direction, PropertyContainer}
import org.neo4j.cypher.internal.commands.Predicate
import collection.Map

class PatterMatchingBuilder(patternGraph: PatternGraph, predicates: Seq[Predicate]) extends MatcherBuilder {
  def getMatches(sourceRow: Map[String, Any]): Traversable[Map[String, Any]] = {
    val bindings: Map[String, Any] = sourceRow.filter(_._2.isInstanceOf[PropertyContainer])
    val boundPairs: Map[String, MatchingPair] = extractBoundMatchingPairs(bindings)

    val undirectedBoundRelationships: Iterable[PatternRelationship] = bindings.keys.
      filter(z => patternGraph.contains(z)).
      filter(patternGraph(_).isInstanceOf[PatternRelationship]).
      map(patternGraph(_).asInstanceOf[PatternRelationship]).
      filter(_.dir == Direction.BOTH)

    val mandatoryPattern: Traversable[Map[String, Any]] = if (undirectedBoundRelationships.isEmpty) {
      createPatternMatcher(boundPairs, false, sourceRow)
    } else {
      val boundRels: Seq[Map[String, MatchingPair]] = createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships, bindings)

      boundRels.map(relMap => createPatternMatcher(relMap ++ boundPairs, false, sourceRow)).reduceLeft(_ ++ _)
    }

    if (patternGraph.containsOptionalElements)
      mandatoryPattern.flatMap(innerMatch => createPatternMatcher(extractBoundMatchingPairs(innerMatch), true, sourceRow))
    else
      mandatoryPattern
  }

  private def createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships: Iterable[PatternRelationship], bindings: Map[String, Any]): Seq[Map[String, MatchingPair]] = {
    val toList = undirectedBoundRelationships.map(patternRel => {
      val rel = bindings(patternRel.key).asInstanceOf[Relationship]
      val x = patternRel.key -> MatchingPair(patternRel, rel)

      // Outputs the first direction of the pattern relationship
      val a1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, rel.getStartNode)
      val a2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, rel.getEndNode)

      // Outputs the second direction of the pattern relationship
      val b1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, rel.getEndNode)
      val b2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, rel.getStartNode)

      Seq(Map(x, a1, a2), Map(x, b1, b2))
    }).toList
    cartesian(toList).map(_.reduceLeft(_ ++ _))
  }

  private def createNullValuesForOptionalElements(matchedGraph: Map[String, Any]): Map[String, Null] = {
    (patternGraph.keySet -- matchedGraph.keySet).map(_ -> null).toMap
  }

  // This method takes  a Seq of Seq and produces the cartesian product of all inner Seqs
  // I'm committing this code, but it's all Tobias' doing.
  private def cartesian[T](lst: Seq[Seq[T]]): Seq[Seq[T]] =
    lst.foldRight(List(List[T]()))(// <- the type T needs to be specified here
      (element: Seq[T], result: List[List[T]]) => // types for better readability
        result.flatMap(r => element.map(e => e :: r))
    ).toSeq

  private def createPatternMatcher(boundPairs: Map[String, MatchingPair], includeOptionals: Boolean, source: Map[String, Any]): Traversable[Map[String, Any]] = {
    val patternMatcher = if (patternGraph.hasDoubleOptionals)
      new DoubleOptionalPatternMatcher(boundPairs, predicates, includeOptionals, source, patternGraph.doubleOptionalPaths)
    else
      new PatternMatcher(boundPairs, predicates, includeOptionals, source)

    if (includeOptionals)
      patternMatcher.map(matchedGraph => matchedGraph ++ createNullValuesForOptionalElements(matchedGraph))
    else
      patternMatcher
  }

  private def extractBoundMatchingPairs(bindings: Map[String, Any]): Map[String, MatchingPair] = bindings.flatMap {
    case (key, value: PropertyContainer) => {
      val element = patternGraph(key)

      value match {
        case node: Node => Seq(key -> MatchingPair(element, node))
        case rel: Relationship => {
          val pr = element.asInstanceOf[PatternRelationship]

          val x = pr.dir match {
            case Direction.OUTGOING => Some((pr.startNode, pr.endNode))
            case Direction.INCOMING => Some((pr.endNode, pr.startNode))
            case Direction.BOTH => None
          }

          //We only want directed bound relationships. Undirected relationship patterns
          //have to be treated a little differently
          x match {
            case Some((a, b)) => {
              val t1 = a.key -> MatchingPair(a, rel.getStartNode)
              val t2 = b.key -> MatchingPair(b, rel.getEndNode)
              val t3 = pr.key -> MatchingPair(pr, rel)

              Seq(t1, t2, t3)
            }
            case None => Seq()
          }
        }
      }


    }
    case _ => Seq()
  }
}

