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

import scala.collection.JavaConverters._
import org.neo4j.graphdb.traversal.{TraversalDescription, Evaluators}
import org.neo4j.graphdb._
import org.neo4j.kernel.{Uniqueness, Traversal}
import org.neo4j.cypher.internal.commands.Predicate

class PatternRelationship(key: String,
                          val startNode: PatternNode,
                          val endNode: PatternNode,
                          val relType: Option[String],
                          val dir: Direction,
                          val optional: Boolean,
                          val predicate: Predicate)
  extends PatternElement(key) {

  def getOtherNode(node: PatternNode) = if (startNode == node) endNode else startNode


  def getGraphRelationships(node: PatternNode, realNode: Node): Seq[GraphRelationship] = {
    val result = (relType match {
      case Some(typeName) => realNode.getRelationships(getDirection(node), DynamicRelationshipType.withName(typeName))
      case None => realNode.getRelationships(getDirection(node))
    }).asScala.toStream.map(new SingleGraphRelationship(_))


    if (startNode == endNode)
      result.filter(r => r.getOtherNode(realNode) == realNode)
    else
      result
  }

  protected def getDirection(node: PatternNode): Direction = {
    dir match {
      case Direction.OUTGOING => if (node == startNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.INCOMING => if (node == endNode) Direction.OUTGOING else Direction.INCOMING
      case Direction.BOTH => Direction.BOTH
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: PatternRelationship => this.key == that.key
    case _ => false
  }

  override def toString = key

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  comingFrom: PatternNode) {

    val moreData = visitRelationship(this, data)

    val otherNode = getOtherNode(comingFrom)

    if (shouldFollow(otherNode)) {
      otherNode.traverse(shouldFollow, visitNode, visitRelationship, moreData)
    }
  }

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T) {

    val moreData = visitRelationship(this, data)

    Seq(startNode, endNode).filter(shouldFollow).foreach(n => n.traverse(shouldFollow, visitNode, visitRelationship, moreData))
  }
}

class VariableLengthPatternRelationship(pathName: String,
                                        val start: PatternNode,
                                        val end: PatternNode,
                                        val relIterable: Option[String],
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        relType: Option[String],
                                        dir: Direction,
                                        optional: Boolean,
                                        predicate: Predicate)
  extends PatternRelationship(pathName, start, end, relType, dir, optional, predicate) {

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
    traversalDescription.traverse(realNode).asScala.toStream.map(p => VariableLengthGraphRelationship(p))
  }
}

