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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{CastSupport, ListSupport}
import org.neo4j.cypher.internal.compiler.v3_2.mutation.GraphElementPropertyFunctions
import org.neo4j.graphdb.Node

case class RemoveLabelsPipe(src: Pipe, variable: String, labels: Seq[LazyLabel])
                        (val estimatedCardinality: Option[Double] = None)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with ListSupport {

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      val item = row.get(variable).get
      if (item != null) removeLabels(row, state, CastSupport.castOrFail[Node](item).getId)
      row
    }
  }

  private def removeLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.flatMap(_.getOptId(state.query)).map(_.id)
    state.query.removeLabelsFromNode(nodeId, labelIds.iterator)
  }

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "RemoveLabels", variables)

  override def symbols = src.symbols

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    RemoveLabelsPipe(onlySource, variable, labels)(estimatedCardinality)
  }
}
