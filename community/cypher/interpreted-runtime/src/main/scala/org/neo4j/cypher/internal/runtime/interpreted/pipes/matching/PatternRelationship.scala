/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyTypes, QueryState}
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.graphdb.Path
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

import scala.collection.JavaConverters._

class PatternRelationship(key: String,
                          val startNode: PatternNode,
                          val endNode: PatternNode,
                          val relTypes: Seq[String],
                          val properties: Map[KeyToken, Expression] = Map.empty,
                          val dir: SemanticDirection)
  extends PatternElement(key) {
  private val types = new LazyTypes(relTypes.toArray)

  def variables2: Map[String, CypherType] = Map(startNode.key -> CTNode, endNode.key -> CTNode, key -> CTRelationship)

  def getOtherNode(node: PatternNode) = if (startNode == node) endNode else startNode

  def getGraphRelationships(node: PatternNode, realNode: NodeValue, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] = {

    val result: Iterator[GraphRelationship] =
      state.query.
        getRelationshipsForIds(realNode.id(), getDirection(node), types.types(state.query))
        .filter(r => canUseThis(r, state, f)).
        map(SingleGraphRelationship)

    if (startNode == endNode)
      result.filter(r => r.getOtherNode(realNode) == realNode).toIndexedSeq
    else
      result.toIndexedSeq
  }

  protected def getDirection(node: PatternNode): SemanticDirection = {
    dir match {
      case OUTGOING => if (node == startNode) OUTGOING else INCOMING
      case INCOMING => if (node == endNode) OUTGOING else INCOMING
      case BOTH     => BOTH
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: PatternRelationship => this.key == that.key
    case _                         => false
  }

  override def toString = key

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  comingFrom: PatternNode,
                  path: Seq[PatternElement]) {
    if (!path.contains(this)) {
      val moreData = visitRelationship(this, data)

      val otherNode = getOtherNode(comingFrom)

      if (shouldFollow(otherNode)) {
        otherNode.traverse(shouldFollow, visitNode, visitRelationship, moreData, path :+ this)
      }
    }
  }

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  path: Seq[PatternElement]) {
    if (!path.contains(this)) {
      val moreData = visitRelationship(this, data)

      Seq(startNode, endNode).filter(shouldFollow).foreach(n => n.traverse(shouldFollow, visitNode, visitRelationship, moreData, path :+ this))
    }
  }

  protected def canUseThis(rel: RelationshipValue, state: QueryState, f: => ExecutionContext): Boolean =
    if (properties.isEmpty) {
      true
    } else {
      val ctx: ExecutionContext = f
      properties.forall {
        case (token, expression) =>
          val propertyId = token.getOptId(state.query)
          if (propertyId.isEmpty) {
            false // The property doesn't exist in the graph
          } else {
            val value = state.query.relationshipOps.getProperty(rel.id, propertyId.get)
            val expectedValue = expression(ctx, state)
            expectedValue == value
          }
      }
    }
}

class VariableLengthPatternRelationship(pathName: String,
                                        val start: PatternNode,
                                        val end: PatternNode,
                                        val relIterable: Option[String],
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        relType: Seq[String],
                                        properties: Map[KeyToken, Expression] = Map.empty,
                                        dir: SemanticDirection)
  extends PatternRelationship(pathName, start, end, relType, properties, dir) {


  override def variables2: Map[String, CypherType] =
    Map(startNode.key -> CTNode,
      endNode.key -> CTNode,
      key -> CTList(CTRelationship)) ++ relIterable.map(_ -> CTList(CTRelationship)).toMap

  override def getGraphRelationships(node: PatternNode, realNode: NodeValue, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] = {
    val matchedPaths: Iterator[Path] = state.query.variableLengthPathExpand(realNode.id(), minHops, maxHops, getDirection(node), relType)

    val filteredPaths = if (properties.isEmpty) {
      matchedPaths
    } else {
      matchedPaths.filter {
        path => path.relationships().iterator().asScala.forall(r => canUseThis(ValueUtils.fromRelationshipProxy(r), state, f))
      }
    }

    filteredPaths.toStream.map(p => VariableLengthGraphRelationship(ValueUtils.fromPath(p)))
  }
}

