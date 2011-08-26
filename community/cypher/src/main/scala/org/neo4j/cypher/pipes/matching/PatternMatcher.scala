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

class PatternMatcher(startPoint: PatternNode) extends Traversable[Map[String, Any]] {

  def foreach[U](f: (Map[String, Any]) => U) {
    traverse(startPoint, startPoint.pinnedEntity.get, Seq(), Seq(), f)
  }

  private def visitNext[U](patternNode: PatternNode,
                           node: Node,
                           pRel: PatternRelationship,
                           history: Seq[MatchingPair],
                           future: Seq[MatchingPair],
                           yielder: (Map[String, Any]) => U) {

    val notVisitedRelationships = patternNode.getRealRelationships(node, pRel, history)
    notVisitedRelationships.foreach(rel => {
      val nextNode = rel.getOtherNode(node)
      val nextPNode = pRel.getOtherNode(patternNode)
      val newHistory = history ++ Seq(MatchingPair(patternNode, node), MatchingPair(pRel, rel))
      traverse(nextPNode, nextNode, newHistory, future, yielder)
    })

  }

  private def yieldThis[U](yielder: Map[String, Any] => U, history: Seq[Any]) {
    val resultMap = history.map(_ match {
      case MatchingPair(p, e) => (p.key, e)
    }).toMap

    yielder(resultMap)
  }

  private def traverse[U](pair: MatchingPair,
                          history: Seq[MatchingPair],
                          future: Seq[MatchingPair],
                          yielder: Map[String, Any] => U) {
    traverse(pair.patternElement.asInstanceOf[PatternNode], pair.entity.asInstanceOf[Node], history, future, yielder)
  }

  private def traverse[U](patternNode: PatternNode,
                          node: Node,
                          history: Seq[MatchingPair],
                          future: Seq[MatchingPair],
                          yielder: Map[String, Any] => U) {

    patternNode.getPRels(history).toList match {
      case pRel :: tail => visitNext(patternNode, node, pRel, history,
        future ++ Seq(MatchingPair(patternNode, node)), yielder)

      case List() => future.toList match {
        case List() => yieldThis(yielder, history ++ Seq(MatchingPair(patternNode, node)))
        case pair :: tail => traverse(pair, history ++ Seq(MatchingPair(patternNode, node)), tail, yielder)
      }
    }
  }
}