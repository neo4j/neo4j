/*
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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{Effects, SetLabel}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.GraphElementPropertyFunctions
import org.neo4j.graphdb.Node

case class SetLabelsPipe(src: Pipe, identifier: String, labels: Seq[LazyLabel])
                        (val estimatedCardinality: Option[Double] = None)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      val nodeId = CastSupport.castOrFail[Node](row.get(identifier).get).getId
      setLabels(row, state, nodeId)
      row
    }
  }

  private def setLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.map(l => {
      val maybeLabelId = l.id(state.query).map(_.id)
      maybeLabelId getOrElse state.query.getOrCreateLabelId(l.name)
    })
    state.query.setLabelsOnNode(nodeId, labelIds.iterator)
  }

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "SetLabels", identifiers)

  override def symbols = src.symbols

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    SetLabelsPipe(onlySource, identifier, labels)(estimatedCardinality)
  }

  override def localEffects = Effects(labels.map(label => SetLabel(label.name)): _*)
}
