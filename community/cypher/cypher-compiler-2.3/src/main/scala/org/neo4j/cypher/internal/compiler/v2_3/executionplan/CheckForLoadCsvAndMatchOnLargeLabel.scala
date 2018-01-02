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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.mutation.{PlainMergeNodeProducer, MergeNodeAction, UpdateAction}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ExecuteUpdateCommandsPipe, LoadCSVPipe, NodeByLabelEntityProducer, NodeStartPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.LabelId
import org.neo4j.cypher.internal.frontend.v2_3.notification.{InternalNotification, LargeLabelWithLoadCsvNotification}

case class CheckForLoadCsvAndMatchOnLargeLabel(planContext: PlanContext, nonIndexedLabelWarningThreshold: Long) extends (Pipe => Option[InternalNotification]) {

  private val threshold = Cardinality(nonIndexedLabelWarningThreshold)

  def apply(pipe: Pipe) = {
    import org.neo4j.cypher.internal.frontend.v2_3.Foldable._

    sealed trait SearchState
    case object NoneFound extends SearchState
    case object LargeLabelFound extends SearchState
    case object LargeLabelWithLoadCsvFound extends SearchState

    // Walk over the pipe tree and check if a large label scan is to be executed after a LoadCsv
    val resultState = pipe.treeFold[SearchState](NoneFound) {
      case _: LoadCSVPipe => (acc, children) =>
        acc match {
          case LargeLabelFound => children(LargeLabelWithLoadCsvFound)
          case e => e
        }
      case NodeStartPipe(_, _, NodeByLabelEntityProducer(_, id), _) if cardinality(id) > threshold =>
        (acc, children) => children(LargeLabelFound)
      case ExecuteUpdateCommandsPipe(_, commands) if hasMergeOnLargeLabel(commands) =>
        (acc, children) => children(LargeLabelFound)
    }

    resultState match {
      case LargeLabelWithLoadCsvFound => Some(LargeLabelWithLoadCsvNotification)
      case _ => None
    }
  }

  private def hasMergeOnLargeLabel(commands: Seq[UpdateAction]) = commands.exists {
    case MergeNodeAction(_, _, _, _, _, _, Some(PlainMergeNodeProducer(NodeByLabelEntityProducer(_, id))))
      if cardinality(id) > threshold => true
    case _ => false
  }

  private def cardinality(id: Int) = planContext.statistics.nodesWithLabelCardinality(Some(LabelId(id)))
}
