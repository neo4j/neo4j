/**
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes.matching

import org.neo4j.graphdb.{NotFoundException, Relationship, Node, PropertyContainer}
import collection.Map
import org.neo4j.cypher.internal.compiler.v1_9.spi.QueryContext

case class MatchingPair(patternElement: PatternElement, entity: Any) {
  def matches(x: Any) = x == entity || x == patternElement || entity == x || patternElement == x

  override def toString = patternElement.key + "=" + entity

  def matchesBoundEntity(boundNodes: Map[String, MatchingPair]): Boolean = boundNodes.get(patternElement.key) match {
    case Some(pinnedNode) => (entity, pinnedNode.entity) match {
      case (a: Node, b: Node)                                                       => a == b
      case (a: SingleGraphRelationship, b: Relationship)                            => a.rel == b
      case (a: Relationship, b: SingleGraphRelationship)                            => a == b.rel
      case (a: VariableLengthGraphRelationship, b: VariableLengthGraphRelationship) => a.path == b.path
      case (a: VariableLengthGraphRelationship, b)                                  => false
      case (a, b: VariableLengthGraphRelationship)                                  => false

    }
    case None             => true
  }

  def getGraphRelationships(pRel: PatternRelationship, ctx: QueryContext): Seq[GraphRelationship] =
    patternElement.asInstanceOf[PatternNode].getGraphRelationships(entity.asInstanceOf[Node], pRel, ctx)

  def getPatternAndGraphPoint: (PatternNode, Node) = (patternElement.asInstanceOf[PatternNode], entity.asInstanceOf[Node])

  def patternNode = patternElement.asInstanceOf[PatternNode]
}
