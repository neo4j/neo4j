/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.planning.notification

import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.notifications.LargeLabelWithLoadCsvNotification

case class checkForLoadCsvAndMatchOnLargeLabel(planContext: PlanContext, nonIndexedLabelWarningThreshold: Long)
    extends NotificationChecker {

  private val threshold = Cardinality(nonIndexedLabelWarningThreshold)

  def apply(plan: LogicalPlan): Seq[InternalNotification] = {

    sealed trait SearchState
    case object NoneFound extends SearchState
    case class LargeLabelFound(labelName: String) extends SearchState
    case class LargeLabelWithLoadCsvFound(labelName: String) extends SearchState

    // Walk over the pipe tree and check if a large label scan is to be executed after a LoadCsv
    val resultState = plan.folder.reverseTreeFold[SearchState](NoneFound) {
      case _: LoadCSV => {
        case LargeLabelFound(labelName) => TraverseChildren(LargeLabelWithLoadCsvFound(labelName))
        case e                          => SkipChildren(e)
      }
      case NodeByLabelScan(_, label, _, _) if cardinality(label.name) > threshold =>
        _ => TraverseChildren(LargeLabelFound(label.name))
    }

    resultState match {
      case LargeLabelWithLoadCsvFound(labelName) => Seq(LargeLabelWithLoadCsvNotification(labelName))
      case _                                     => Seq.empty
    }
  }

  private def cardinality(labelName: String): Cardinality =
    planContext.statistics.nodesWithLabelCardinality(planContext.getOptLabelId(labelName).map(LabelId))
}
