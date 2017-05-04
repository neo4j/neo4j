/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.pipes

import org.neo4j.cypher.internal.compiler.v3_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.collection.PrefetchingIterator

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
  protected def findRelationships(query: QueryContext, fromNode: Node, toNode: Node,
                                relCache: RelationshipsCache, dir: SemanticDirection, relTypes: => Option[Seq[Int]]): Iterator[Relationship] = {

    val fromNodeIsDense = query.nodeIsDense(fromNode.getId)
    val toNodeIsDense = query.nodeIsDense(toNode.getId)

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

  private def relIterator(query: QueryContext, fromNode: Node,  toNode: Node, preserveDirection: Boolean,
                          relTypes: Option[Seq[Int]], relCache: RelationshipsCache, dir: SemanticDirection) = {
    val (start, localDirection, end) = if(preserveDirection) (fromNode, dir, toNode) else (toNode, dir.reversed, fromNode)
    val relationships = query.getRelationshipsForIds(start, localDirection, relTypes)
    new PrefetchingIterator[Relationship] {
      //we do not expect two nodes to have many connecting relationships
      val connectedRelationships = new ArrayBuffer[Relationship](2)

      override def fetchNextOrNull(): Relationship = {
        while (relationships.hasNext) {
          val rel = relationships.next()
          val other = rel.getOtherNode(start)
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

  private def getDegree(node: Node, relTypes: Option[Seq[Int]], direction: SemanticDirection, query: QueryContext) = {
    relTypes.map {
      case rels if rels.isEmpty   => query.nodeGetDegree(node.getId, direction)
      case rels if rels.size == 1 => query.nodeGetDegree(node.getId, direction, rels.head)
      case rels                   => rels.foldLeft(0)(
        (acc, rel)                => acc + query.nodeGetDegree(node.getId, direction, rel)
      )
    }.getOrElse(query.nodeGetDegree(node.getId, direction))
  }

  @inline
  protected def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null    => null
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  protected final class RelationshipsCache(capacity: Int) {

    val table = new mutable.OpenHashMap[(Long, Long), Seq[Relationship]]()

    def get(start: Node, end: Node, dir: SemanticDirection): Option[Seq[Relationship]] = table.get(key(start, end, dir))

    def put(start: Node, end: Node, rels: Seq[Relationship], dir: SemanticDirection) = {
      if (table.size < capacity) {
        table.put(key(start, end, dir), rels)
      }
    }

    @inline
    private def key(start: Node, end: Node, dir: SemanticDirection) = {
      // if direction is BOTH than we keep the key sorted, otherwise direction is important and we keep key as is
      if (dir != SemanticDirection.BOTH) (start.getId, end.getId)
      else {
        if (start.getId < end.getId)
          (start.getId, end.getId)
        else
          (end.getId, start.getId)
      }
    }
  }
}
