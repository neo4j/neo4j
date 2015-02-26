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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.LRUCache
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, InternalException}
import org.neo4j.graphdb.{Direction, Node, Relationship}
import org.neo4j.helpers.collection.PrefetchingIterator

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Expand when both end-points are known, find all relationships of the given
 * type in the given direction between the two end-points.
 */
case class ExpandIntoPipe(source: Pipe,
                          fromName: String,
                          relName: String,
                          toName: String,
                          dir: Direction,
                          types: LazyTypes)(val estimatedCardinality: Option[Double] = None)
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
  private def findRelationships(query: QueryContext, fromNode: Node, toNode: Node, relCache: RelationshipsCache) = {
    //check degree and iterate from the node with smaller degree
    val relTypes = types.types(query)
    val fromDegree = getDegree(fromNode, relTypes, dir, query)
    val toDegree = getDegree(toNode, relTypes, dir.reverse, query)

    val (start, end, relationships) = if (fromDegree < toDegree)
      (fromNode, toNode, query.getRelationshipsForIds(fromNode, dir, relTypes))
    else
      (toNode, fromNode, query.getRelationshipsForIds(toNode, dir.reverse(), relTypes))

    if (math.min(fromDegree, toDegree) == 0) {
      relCache.put(start, end, Seq.empty)
      Iterator.empty
    } else {
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
        relCache.put(start, end, connectedRelationships)
        null
      }
    }
    }.asScala
  }

  private def getDegree(node: Node, relTypes: Option[Seq[Int]], direction: Direction, query: QueryContext) = {
    relTypes.map {
      case rels if rels.isEmpty   => query.nodeGetDegree(node.getId, direction)
      case rels if rels.size == 1 => query.nodeGetDegree(node.getId, direction, rels.head)
      case rels                   => rels.foldLeft(0)(
        (acc, rel)                => acc + query.nodeGetDegree(node.getId, direction, rel)
      )
    }.getOrElse(query.nodeGetDegree(node.getId, direction))
  }

  def typeNames = types.names

  @inline
  private def getRowNode(row: ExecutionContext, col: String): Node = {
    row.getOrElse(col, throw new InternalException(s"Expected to find a node at $col but found nothing")) match {
      case n: Node => n
      case null    => null
      case value   => throw new InternalException(s"Expected to find a node at $col but found $value instead")
    }
  }

  def planDescription = {
    source.planDescription.andThen(this, "Expand(Into)", identifiers, ExpandExpression(fromName, relName, typeNames, toName, dir))
  }

  val symbols = source.symbols.add(toName, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects.READS_ENTITIES

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private final class RelationshipsCache(capacity: Int) {
    private val table = new LRUCache[(Node, Node), Seq[Relationship]](capacity)

    def put(node1: Node, node2: Node, rels: Seq[Relationship]) = table.put(key(node1, node2), rels)

    def get(node1: Node, node2: Node) = table.get(key(node1, node2))

    @inline
    private def key(node1: Node, node2: Node) =
      if (node1.getId < node2.getId) (node1, node2) else (node2, node1)
  }
}
