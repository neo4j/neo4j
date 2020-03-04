/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.internal.helpers.collection.PrefetchingIterator
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

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
  protected def findRelationships(state: QueryState,
                                  fromNode: NodeValue,
                                  toNode: NodeValue,
                                  relCache: RelationshipsCache,
                                  dir: SemanticDirection,
                                  relTypes: => Array[Int]): Iterator[RelationshipValue] = {

    val fromNodeIsDense = state.query.nodeIsDense(fromNode.id(), state.cursors.nodeCursor)
    val toNodeIsDense = state.query.nodeIsDense(toNode.id(), state.cursors.nodeCursor)

    //if both nodes are dense, start from the one with the lesser degree
    if (fromNodeIsDense && toNodeIsDense) {
      //check degree and iterate from the node with smaller degree
      val fromDegree = getDegree(fromNode, relTypes, dir, state)
      if (fromDegree == 0) {
        return Iterator.empty
      }

      val toDegree = getDegree(toNode, relTypes, dir.reversed, state)
      if (toDegree == 0) {
        return Iterator.empty
      }

      relIterator(state.query, fromNode, toNode, preserveDirection = fromDegree < toDegree, relTypes, relCache, dir)
    }
    // iterate from a non-dense node
    else if (toNodeIsDense)
      relIterator(state.query, fromNode, toNode, preserveDirection = true, relTypes, relCache, dir)
    else if (fromNodeIsDense)
      relIterator(state.query, fromNode, toNode, preserveDirection = false, relTypes, relCache, dir)
    //both nodes are non-dense, choose a random starting point
    else
      relIterator(state.query, fromNode, toNode, alternate(), relTypes, relCache, dir)
  }

  private var alternateState = false

  private def alternate(): Boolean = {
    val result = !alternateState
    alternateState = result
    result
  }

  private def relIterator(query: QueryContext, fromNode: NodeValue,  toNode: NodeValue, preserveDirection: Boolean,
                          relTypes: Array[Int], relCache: RelationshipsCache, dir: SemanticDirection): Iterator[RelationshipValue] = {
    val (start, localDirection, end) = if(preserveDirection) (fromNode, dir, toNode) else (toNode, dir.reversed, fromNode)
    val relationships = query.getRelationshipsForIds(start.id(), localDirection, relTypes)
    new PrefetchingIterator[RelationshipValue] {
      val connectedRelationships = new RelationshipChain
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

  private def getDegree(node: NodeValue, relTypes: Array[Int], direction: SemanticDirection, state: QueryState) = {
    if (relTypes == null) state.query.nodeGetDegree(node.id(), direction, state.cursors.nodeCursor)
    else if (relTypes.length == 1) state.query.nodeGetDegree(node.id(), direction, relTypes(0), state.cursors.nodeCursor)
    else {
      relTypes.foldLeft(0)(
        (acc, rel) => acc + state.query.nodeGetDegree(node.id(), direction, rel, state.cursors.nodeCursor)
        )
    }
  }

  @inline
  protected def getRowNode(row: ExecutionContext, col: String): AnyValue = {
    row.getByName(col) match {
      case n: NodeValue => n
      case IsNoValue() => NO_VALUE
      case value => throw new ParameterWrongTypeException(s"Expected to find a node at '$col' but found $value instead")
    }
  }

  final class RelationshipChain() {
    //we do not expect two nodes to have many connecting relationships
    private val buffer = new ArrayBuffer[RelationshipValue](2)
    private var heapUsageEstimation = 0L

    def append(rel: RelationshipValue): Unit = {
      buffer.append(rel)
      heapUsageEstimation += rel.estimatedHeapUsage()
    }

    def estimatedHeapUsage: Long = heapUsageEstimation

    def relationships: Seq[RelationshipValue] = buffer
  }

  protected final class RelationshipsCache(capacity: Int, memoryTracker: QueryMemoryTracker) {

    val table = new mutable.OpenHashMap[(Long, Long), Seq[RelationshipValue]]()

    def get(start: NodeValue, end: NodeValue, dir: SemanticDirection): Option[Seq[RelationshipValue]] = table.get(key(start, end, dir))

    /**
      * Add connections to the cache.
      * @param start the start node
      * @param end the end node
      * @param rels the interconnecting relationships
      * @param dir the direction of the interconnecting relationships
      */
    def put(start: NodeValue, end: NodeValue, rels: RelationshipChain, dir: SemanticDirection): Unit = {
      if (table.size < capacity) {
        //key uses two longs
        memoryTracker.allocated(2 * java.lang.Long.BYTES)
        memoryTracker.allocated(rels.estimatedHeapUsage)
        table.put(key(start, end, dir), rels.relationships)
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
