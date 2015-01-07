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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import scala.collection.JavaConverters._
import org.neo4j.graphdb.traversal.{TraversalDescription, Evaluators}
import org.neo4j.graphdb._
import org.neo4j.kernel.{Uniqueness, Traversal}
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.symbols.RelationshipType
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext

class PatternRelationship(key: String,
                          val startNode: PatternNode,
                          val endNode: PatternNode,
                          val relTypes: Seq[String],
                          val properties: Map[KeyToken, Expression] = Map.empty,
                          val dir: Direction)
  extends PatternElement(key) {

  def identifiers2: Map[String, CypherType] = Map(startNode.key -> CTNode, endNode.key -> CTNode, key -> CTRelationship)

  def getOtherNode(node: PatternNode) = if (startNode == node) endNode else startNode

  def getGraphRelationships(node: PatternNode, realNode: Node, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] = {

    val result: Iterator[GraphRelationship] =
      state.query.
        getRelationshipsFor(realNode, getDirection(node), relTypes).
        filter(r => canUseThis(r, state, f)).
        map(new SingleGraphRelationship(_))

    if (startNode == endNode)
      result.filter(r => r.getOtherNode(realNode) == realNode).toSeq
    else
      result.toSeq
  }

  protected def getDirection(node: PatternNode): Direction = {
    dir match {
      case Direction.OUTGOING => if (node == startNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.INCOMING => if (node == endNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.BOTH     => Direction.BOTH
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

  protected def canUseThis(rel: Relationship, state: QueryState, f: => ExecutionContext): Boolean =
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
            val value = state.query.relationshipOps.getProperty(rel.getId, propertyId.get)
            val expectedValue = expression(ctx)(state)
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
                                        dir: Direction)
  extends PatternRelationship(pathName, start, end, relType, properties, dir) {


  override def identifiers2: Map[String, CypherType] =
    Map(startNode.key -> CTNode,
      endNode.key -> CTNode,
      key -> CTCollection(CTRelationship)) ++ relIterable.map(_ -> CTCollection(CTRelationship)).toMap

  override def getGraphRelationships(node: PatternNode, realNode: Node, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] = {

    val depthEval = (minHops, maxHops) match {
      case (None, None)           => Evaluators.fromDepth(1)
      case (Some(min), None)      => Evaluators.fromDepth(min)
      case (None, Some(max))      => Evaluators.includingDepths(1, max)
      case (Some(min), Some(max)) => Evaluators.includingDepths(min, max)
    }

    val baseTraversalDescription: TraversalDescription = Traversal.description()
      .evaluator(depthEval)
      .uniqueness(Uniqueness.RELATIONSHIP_PATH)

    val traversalDescription = if (relType.isEmpty) {
      baseTraversalDescription.expand(Traversal.expanderForAllTypes(getDirection(node)))
    } else {
      val emptyExpander = Traversal.emptyExpander()
      val dir = getDirection(node)
      val expander = relType.foldLeft(emptyExpander) {
        case (e, t) => e.add(DynamicRelationshipType.withName(t), dir)
      }
      baseTraversalDescription.expand(expander)
    }

    val matchedPaths = traversalDescription.traverse(realNode).asScala

    val filteredPaths = if (properties.isEmpty) {
      matchedPaths
    } else {
      matchedPaths.filter {
        path => path.relationships().iterator().asScala.forall(r => canUseThis(r, state, f))
      }
    }

    filteredPaths.toStream.map(p => VariableLengthGraphRelationship(p))
  }
}

