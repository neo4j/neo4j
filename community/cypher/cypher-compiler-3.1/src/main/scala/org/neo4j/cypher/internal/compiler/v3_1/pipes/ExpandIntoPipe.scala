/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.graphdb.Node

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
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe with CachingExpandInto {
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
              val relationships = relCache.get(fromNode, toNode, dir)
                .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

              if (relationships.isEmpty) Iterator.empty
              else relationships.map(row.newWith1(relName, _))
            }

          case null =>
            Iterator.empty
        }
    }
  }

  def planDescriptionWithoutCardinality = {
    val expandDescr = ExpandExpression(fromName, relName, lazyTypes.names, toName, dir, minLength = 1, Some(1))
    source.planDescription.andThen(this.id, "Expand(Into)", variables, expandDescr)
  }

  val symbols = source.symbols.add(fromName, CTNode).add(toName, CTNode).add(relName, CTRelationship)

  override def localEffects = Effects(ReadsAllNodes, ReadsAllRelationships)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

}


