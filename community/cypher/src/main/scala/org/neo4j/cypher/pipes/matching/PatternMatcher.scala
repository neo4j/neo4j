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

import org.neo4j.graphdb.Node

class PatternMatcher(startPoint: PatternNode, bindings: Map[String, Any]) extends Traversable[Map[String, Any]] {

  def foreach[U](f: (Map[String, Any]) => U) {
    traverse(MatchingPair(startPoint, startPoint.pinnedEntity.get), Seq(), Seq(), f)
  }

  private def traverse[U](current: MatchingPair,
                          history: Seq[MatchingPair],
                          future: Seq[MatchingPair],
                          yielder: Map[String, Any] => U) {
//    println(String.format("traverse(current=%s | history=%s | future=%s)", current, history, future))

    val patternNode: PatternNode = current.patternElement.asInstanceOf[PatternNode]
    val node: Node = current.entity.asInstanceOf[Node]

    bindings.get(patternNode.key) match {
      case Some(pinnedNode) => if (pinnedNode != node) return
      case None =>
    }

    patternNode.getPRels(history).toList match {
      case pRel :: tail => visitNext(current, pRel, history, future ++ Seq(current), yielder)

      case List() => future.toList match {
        case List() => yieldThis(yielder, history ++ Seq(current))
        case next :: rest => traverse(next, history ++ Seq(current), rest, yielder)
      }
    }
  }

  private def visitNext[U](current: MatchingPair,
                           pRel: PatternRelationship,
                           history: Seq[MatchingPair],
                           future: Seq[MatchingPair],
                           yielder: (Map[String, Any]) => U) {
//    println(String.format("visitNext(current=%s | pRel=%s | history=%s | future=%s)", current, pRel, history, future))

    val patternNode: PatternNode = current.patternElement.asInstanceOf[PatternNode]
    val node: Node = current.entity.asInstanceOf[Node]

    val notVisitedRelationships = patternNode.getGraphRelationships(node, pRel, history)
    notVisitedRelationships.foreach(rel => {
      val nextNode = rel.getOtherNode(node)
      val nextPNode = pRel.getOtherNode(patternNode)
      val newHistory = history ++ Seq(current, MatchingPair(pRel, rel))
      traverse(MatchingPair(nextPNode, nextNode), newHistory, future, yielder)
    })

  }

  private def yieldThis[U](yielder: Map[String, Any] => U, history: Seq[Any]) {
    val resultMap = history.flatMap(_ match {
      case MatchingPair(p,e) => (p,e) match {
        case (pe:PatternNode, entity:Node) => Seq(pe.key -> entity)
        case (pe:PatternRelationship, entity:SingleGraphRelationship) => Seq(pe.key -> entity.rel)
        case (pe:VariableLengthPatternRelationship, entity:VariableLengthGraphRelationship) => Seq(
            pe.start.key -> entity.path.startNode(),
            pe.end.key -> entity.path.endNode()
          //          pe.key -> entity.path
        )
      }
    }).toMap
//    println(String.format("yield(history=%s) => %s", history, resultMap))

    yielder(resultMap)
  }
}