/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

import scala.collection.Map

case class MatchingPair(patternElement: PatternElement, entity: Any) {
  def matches(x: Any) = x == entity || x == patternElement || entity == x || patternElement == x

  override def toString = patternElement.key + "=" + entity

  def matchesBoundEntity(boundNodes: Map[String, Set[MatchingPair]]): Boolean = boundNodes.get(patternElement.key)
  match {
    case Some(pinnedNodeSet) => pinnedNodeSet.forall(pinnedNode => (entity, pinnedNode.entity) match {
      case (a: NodeValue, b: NodeValue)                                                       => a == b
      case (a: SingleGraphRelationship, b: RelationshipValue)                            => a.rel == b
      case (a: RelationshipValue, b: SingleGraphRelationship)                            => a == b.rel
      case (a: VariableLengthGraphRelationship, b: VariableLengthGraphRelationship) => a.path == b.path
      case (a: VariableLengthGraphRelationship, _)                                  => false
      case (a, _: VariableLengthGraphRelationship)                                  => false

    })
    case None             => true
  }

  def getGraphRelationships(pRel: PatternRelationship, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] =
    patternElement.asInstanceOf[PatternNode].getGraphRelationships(entity.asInstanceOf[NodeValue], pRel, state, f)

  def getPatternAndGraphPoint: (PatternNode, NodeValue) = (patternElement.asInstanceOf[PatternNode], entity.asInstanceOf[NodeValue])

  def patternNode = patternElement.asInstanceOf[PatternNode]
}
