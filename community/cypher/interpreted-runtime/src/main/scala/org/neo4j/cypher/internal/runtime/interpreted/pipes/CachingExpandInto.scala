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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.expressions.SemanticDirection
import org.neo4j.helpers.collection.PrefetchingIterator
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Used by pipes that needs to expand between two known nodes.
 *
 * Given a pattern (a)-->(b) it will do the following:
 * - if both a and b are dense nodes, start from the one with the lesser degree
 * - if just one of the nodes is dense, start from the non-dense node
 * - if both are non-dense, randomly pick one or the other
 * - cache all found connecting relationships.
 *
 */
trait CachingExpandInto {

  /**
   * Finds all relationships connecting fromNode and toNode.
   */
  protected def findRelationships(query: QueryContext, fromNode: NodeValue, toNode: NodeValue,
                                relCache: RelationshipsCache, dir: SemanticDirection, relTypes: => Option[Array[Int]]): Iterator[RelationshipValue] = {

    val fromNodeIsDense = query.nodeIsDense(fromNode.id())
    val toNodeIsDense = query.nodeIsDense(toNode.id())

    //if both nodes are dense, start from the one with the lesser degree
    if (fromNodeIsDense && toNodeIsDense) {
      //check degree and iterate from the node with smaller degree
      val fromDegree = getDegree(fromNode, relTypes, dir, query)
      if (fromDegree == 0) {
        return Iterator.empty
      }

      val toDegree = getDegree(toNode, relTypes, dir.reversed, query)
      if (toDegree == 0) {
        return Iterator.empty
      }

      relIterator(query, fromNode, toNode, preserveDirection = fromDegree < toDegree, relTypes, relCache, dir)
    }
    // iterate from a non-dense node
    else if (toNodeIsDense)
      relIterator(query, fromNode, toNode, preserveDirection = true, relTypes, relCache, dir)
    else if (fromNodeIsDense)
      relIterator(query, fromNode, toNode, preserveDirection = false, relTypes, relCache, dir)
    //both nodes are non-dense, choose a random starting point
    else
      relIterator(query, fromNode, toNode, alternate(), relTypes, relCache, dir)
  }

  private var alternateState = false

  private def alternate(): Boolean = {
    val result = !alternateState
    alternateState = result
    result
  }

  private def relIterator(query: QueryContext, fromNode: NodeValue,  toNode: NodeValue, preserveDirection: Boolean,
                          relTypes: Option[Array[Int]], relCache: RelationshipsCache, dir: SemanticDirection) = {
    val (start, localDirection, end) = if(preserveDirection) (fromNode, dir, toNode) else (toNode, dir.reversed, fromNode)
    val relationships = query.getRelationshipsForIds(start.id(), localDirection, relTypes)
    new PrefetchingIterator[RelationshipValue] {
      //we do not expect two nodes to have many connecting relationships
      val connectedRelationships = new ArrayBuffer[RelationshipValue](2)

      override def fetchNextOrNull(): RelationshipValue = {
        while (relationships.hasNext) {
          val rel = relationships.next()
          val other = rel.otherNode(start)
          if (end == other) {
            connectedRelationships.append(rel)
            return rel
          }
        }
        relCache.put(fromNode, toNode, connectedRelationships, dir)
        null
      }
    }.asScala
  }

  private def getDegree(node: NodeValue, relTypes: Option[Array[Int]], direction: SemanticDirection, query: QueryContext) = {
    relTypes.map {
      case rels if rels.isEmpty   => query.nodeGetDegree(node.id(), direction)
      case rels if rels.length == 1 => query.nodeGetDegree(node.id(), direction, rels.head)
      case rels                   => rels.foldLeft(0)(
        (acc, rel)                => acc + query.nodeGetDegree(node.id(), direction, rel)
      )
    }.getOrElse(query.nodeGetDegree(node.id(), direction))
  }

  @inline
  protected def getRowNode(row: ExecutionContext, col: String): AnyValue = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: NodeValue => n
      case NO_VALUE    => NO_VALUE
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  protected final class RelationshipsCache(capacity: Int) {

    val table = new mutable.OpenHashMap[(Long, Long), Seq[RelationshipValue]]()

    def get(start: NodeValue, end: NodeValue, dir: SemanticDirection): Option[Seq[RelationshipValue]] = table.get(key(start, end, dir))

    def put(start: NodeValue, end: NodeValue, rels: Seq[RelationshipValue], dir: SemanticDirection) = {
      if (table.size < capacity) {
        table.put(key(start, end, dir), rels)
      }
    }

    @inline
    private def key(start: NodeValue, end: NodeValue, dir: SemanticDirection) = {
      // if direction is BOTH than we keep the key sorted, otherwise direction is important and we keep key as is
      if (dir != SemanticDirection.BOTH) (start.id, end.id())
      else {
        if (start.id() < end.id())
          (start.id(), end.id())
        else
          (end.id(), start.id())
      }
    }
  }
}
