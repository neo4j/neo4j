/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, InternalException}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.collection.PrefetchingIterator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Expand when both end-points are known, find all relationships of the given
 * type in the given direction between the two end-points.
 *
 * This is done by checking both nodes and starts from any non-dense node of the two.
 * If both nodes are dense, we find the degree of each and expand from the smaller of the two
 *
 * This pipe also caches relationship information between nodes for the duration of the query
 */
case class ExpandIntoPipe(source: Pipe,
                          fromName: String,
                          relName: String,
                          toName: String,
                          dir: SemanticDirection,
                          lazyTypes: LazyTypes)(val estimatedCardinality: Option[Double] = None)
                         (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  self =>

  private final val CACHE_SIZE = 100000

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new RelationshipsCache(CACHE_SIZE)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: Node =>
            val toNode = getRowNode(row, toName)

            if (toNode == null) Iterator.empty
            else {
              val relationships = relCache.get(fromNode, toNode)
                .getOrElse(findRelationships(state.query, fromNode, toNode, relCache))

              if (relationships.isEmpty) Iterator.empty
              else relationships.map(row.newWith2(relName, _, toName, toNode))
            }

          case null =>
            Iterator.empty
        }
    }
  }

  /**
   * Finds all relationships connecting fromNode and toNode.
   */
  private def findRelationships(query: QueryContext, fromNode: Node, toNode: Node,
                                relCache: RelationshipsCache): Iterator[Relationship] = {
    val relTypes = lazyTypes.types(query)

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

      relIterator(query, fromNode, toNode, preserveDirection = fromDegree < toDegree, relTypes, relCache)
    }
    // iterate from a non-dense node
    else if (toNodeIsDense)
      relIterator(query, fromNode, toNode, preserveDirection = true, relTypes, relCache)
    else if (fromNodeIsDense)
      relIterator(query, fromNode, toNode, preserveDirection = false, relTypes, relCache)
    //both nodes are non-dense, choose a starting point by alternating from and to nodes
    else
      relIterator(query, fromNode, toNode, preserveDirection = alternate(), relTypes, relCache)
  }

  private var alternateState = false

  private def alternate(): Boolean = {
    val result = !alternateState
    alternateState = result
    result
  }

  private def relIterator(query: QueryContext, fromNode: Node,  toNode: Node, preserveDirection: Boolean,
                          relTypes: Option[Seq[Int]], relCache: RelationshipsCache) = {
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
        relCache.put(fromNode, toNode, connectedRelationships)
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
  private def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null    => null
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  def planDescriptionWithoutCardinality =
    source.planDescription.andThen(this.id, "Expand(Into)", identifiers, ExpandExpression(fromName, relName, lazyTypes.names, toName, dir))

  val symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects(ReadsAllNodes, ReadsRelationships)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private final class RelationshipsCache(capacity: Int) {

    val table = new mutable.OpenHashMap[(Long, Long), Seq[Relationship]]()

    def get(start: Node, end: Node): Option[Seq[Relationship]] = table.get(key(start, end))

    def put(start: Node, end: Node, rels: Seq[Relationship]) = {
      if (table.size < capacity) {
        table.put(key(start, end), rels)
      }
    }

    @inline
    private def key(start: Node, end: Node) = {
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
