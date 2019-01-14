/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

import scala.collection.mutable

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
trait PrimitiveCachingExpandInto {

  /**
    * Finds all relationships connecting fromNode and toNode.
    */
  protected def findRelationships(query: QueryContext, fromNode: Long, toNode: Long,
                                  relCache: PrimitiveRelationshipsCache, dir: SemanticDirection, relTypes: => Option[Array[Int]]): PrimitiveLongIterator = {

    val fromNodeIsDense = query.nodeIsDense(fromNode)
    val toNodeIsDense = query.nodeIsDense(toNode)

    //if both nodes are dense, start from the one with the lesser degree
    if (fromNodeIsDense && toNodeIsDense) {
      //check degree and iterate from the node with smaller degree
      val fromDegree = getDegree(fromNode, relTypes, dir, query)
      if (fromDegree == 0) {
        return RelationshipIterator.EMPTY
      }

      val toDegree = getDegree(toNode, relTypes, dir.reversed, query)
      if (toDegree == 0) {
        return RelationshipIterator.EMPTY
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

  private def relIterator(query: QueryContext, fromNode: Long, toNode: Long, preserveDirection: Boolean,
                          relTypes: Option[Array[Int]], relCache: PrimitiveRelationshipsCache, dir: SemanticDirection): PrimitiveLongIterator = {
    val (start, localDirection, end) = if (preserveDirection) (fromNode, dir, toNode) else (toNode, dir.reversed, fromNode)
    val relationships: RelationshipIterator = query.getRelationshipsForIdsPrimitive(start, localDirection, relTypes)

    val connectedRelationships = mutable.ArrayBuilder.make[Long]
    var connected: Boolean = false

    val relVisitor = new RelationshipVisitor[InternalException] {



      override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit =
        if ((end == startNodeId && start == endNodeId) || (start == startNodeId && end == endNodeId)) {
          connectedRelationships += relationshipId
          connected = true
        }
    }

    new PrimitiveLongIterator {
      var nextRelId: Long = -1
      // used to ensure consecutive calls to hasNext(), without interleaved next(),
      // return same result & don't consume additional inner iterator elements
      var consumed: Boolean = true

      override def next: Long = {
        consumed = true
        nextRelId
      }

      override def hasNext: Boolean = !consumed || computeNext

      private def computeNext = {
        connected = false
        while (relationships.hasNext && !connected) {
          nextRelId = relationships.next()
          relationships.relationshipVisit(nextRelId, relVisitor)
        }
        if (!relationships.hasNext) {
          relCache.put(fromNode, toNode, connectedRelationships.result(), dir)
        }
        // if connected is true, the following next() invocation has something to return
        // if connected is false, we have exhausted the iterator and nothing more needs to be returned
        consumed = !connected
        connected
      }
    }
  }

  private def getDegree(node: Long, relTypes: Option[Array[Int]], direction: SemanticDirection, query: QueryContext) = {
    relTypes.map {
      case rels if rels.isEmpty => query.nodeGetDegree(node, direction)
      case rels if rels.length == 1 => query.nodeGetDegree(node, direction, rels.head)
      case rels => rels.foldLeft(0)(
        (acc, rel) => acc + query.nodeGetDegree(node, direction, rel)
      )
    }.getOrElse(query.nodeGetDegree(node, direction))
  }
}

protected final class PrimitiveRelationshipsCache(capacity: Int) {

  val table = new mutable.OpenHashMap[(Long, Long), Array[Long]]()

  def get(start: Long, end: Long, dir: SemanticDirection): Option[PrimitiveLongIterator] = {
    table.get(key(start, end, dir)).map(rels => {
      new PrimitiveLongIterator {
        var index: Int = 0

        override def next(): Long = {
          val r = rels(index)
          index = index + 1
          r
        }

        override def hasNext: Boolean = index < rels.length
      }
    })
  }

  def put(start: Long, end: Long, rels: Array[Long], dir: SemanticDirection): Any = {
    if (table.size < capacity) {
      table.put(key(start, end, dir), rels)
    }
  }

  @inline
  private def key(start: Long, end: Long, dir: SemanticDirection) = {
    // if direction is BOTH than we keep the key sorted, otherwise direction is important and we keep key as is
    if (dir != SemanticDirection.BOTH) (start, end)
    else {
      if (start < end)
        (start, end)
      else
        (end, start)
    }
  }
}
