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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes._
import org.neo4j.cypher.internal.compiler.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_4.LabelId
import org.neo4j.cypher.internal.frontend.v3_4.notification.{InternalNotification, LargeLabelWithLoadCsvNotification}
import org.neo4j.cypher.internal.ir.v3_4.Cardinality

case class CheckForLoadCsvAndMatchOnLargeLabel(planContext: PlanContext, nonIndexedLabelWarningThreshold: Long) extends (Pipe => Option[InternalNotification]) {

  private val threshold = Cardinality(nonIndexedLabelWarningThreshold)

  def apply(pipe: Pipe) = {
    import org.neo4j.cypher.internal.util.v3_4.Foldable._

    sealed trait SearchState
    case object NoneFound extends SearchState
    case object LargeLabelFound extends SearchState
    case object LargeLabelWithLoadCsvFound extends SearchState

    // Walk over the pipe tree and check if a large label scan is to be executed after a LoadCsv
    val resultState = pipe.reverseTreeFold[SearchState](NoneFound) {
      case _: LoadCSVPipe => {
        case LargeLabelFound => (LargeLabelWithLoadCsvFound, Some(identity))
        case e => (e, None)
      }
      case NodeByLabelScanPipe(_, label) if cardinality(label.getOptId(planContext)) > threshold =>
        acc => (LargeLabelFound, Some(identity))
      case NodeStartPipe(_, _, NodeByLabelEntityProducer(_, id), _) if cardinality(id) > threshold =>
        acc => (LargeLabelFound, Some(identity))
    }

    resultState match {
      case LargeLabelWithLoadCsvFound => Some(LargeLabelWithLoadCsvNotification)
      case _ => None
    }
  }

  private def cardinality(id: Int) = planContext.statistics.nodesWithLabelCardinality(Some(LabelId(id)))
}
