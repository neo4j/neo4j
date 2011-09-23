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

import scala.collection.JavaConverters._
import org.neo4j.graphdb.traversal.{TraversalDescription, Evaluators}
import org.neo4j.graphdb._
import org.neo4j.kernel.{Uniqueness, Traversal}

class PatternRelationship(key: String,
                          val leftNode: PatternNode,
                          val rightNode: PatternNode,
                          relType: Option[String],
                          dir: Direction,
                          val optional:Boolean)
  extends PatternElement(key)
  with PinnablePatternElement[Relationship] {

  def getOtherNode(node: PatternNode) = if(leftNode==node) rightNode else leftNode
  def getGraphRelationships(node: PatternNode, realNode: Node): Seq[GraphRelationship] = {
    (relType match {
      case Some(typeName) => realNode.getRelationships(getDirection(node), DynamicRelationshipType.withName(typeName))
      case None => realNode.getRelationships(getDirection(node))
    }).asScala.map(new SingleGraphRelationship(_)).toSeq
  }
  protected def getDirection(node: PatternNode): Direction = {
    dir match {
      case Direction.OUTGOING => if (node == leftNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.INCOMING => if (node == rightNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.BOTH => Direction.BOTH
    }
  }

  override def toString = key
}

class VariableLengthPatternRelationship( pathName: String,
                                         val start: PatternNode,
                                         val end: PatternNode,
                                         minHops: Option[Int],
                                         maxHops: Option[Int],
                                         relType: Option[String],
                                         dir: Direction,
                                         optional:Boolean)
  extends PatternRelationship(pathName, start, end, relType, dir, optional) {

  override def getGraphRelationships(node: PatternNode, realNode: Node): Seq[GraphRelationship] = {

    val depthEval = (minHops, maxHops) match {
      case (None, None) => Evaluators.fromDepth(1)
      case (Some(min), None) => Evaluators.fromDepth(min)
      case (None, Some(max)) => Evaluators.includingDepths(1, max)
      case (Some(min), Some(max)) => Evaluators.includingDepths(min, max)
    }

    val baseTraversalDescription: TraversalDescription = Traversal.description()
      .evaluator(depthEval)
      .uniqueness(Uniqueness.RELATIONSHIP_PATH)

    val traversalDescription = relType match {
      case Some(typeName) => baseTraversalDescription.expand(Traversal.expanderForTypes(DynamicRelationshipType.withName(typeName), getDirection(node)))
      case None => baseTraversalDescription.expand(Traversal.expanderForAllTypes(getDirection(node)))
    }
    traversalDescription.traverse(realNode).asScala.toSeq.map(p => VariableLengthGraphRelationship(p))
  }
}

