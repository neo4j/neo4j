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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case class labelScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    qg.selections.flatPredicatesSet.flatMap {
      case labelPredicate @ HasLabels(variable: Variable, labels)
        if !skipIDs.contains(variable) && qg.patternNodes(variable) && !qg.argumentIds(variable) =>
        context.staticComponents.planContext.nodeTokenIndex.map { nodeTokenIndex =>
          val labelName = labels.head
          val hint = qg.hints.collectFirst {
            case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(labelName.name)) => hint
          }
          val providedOrder = ResultOrdering.providedOrderForLabelScan(
            interestingOrderConfig.orderToSolve,
            variable,
            nodeTokenIndex.orderCapability,
            context.providedOrderFactory
          )
          context.staticComponents.logicalPlanProducer.planNodeByLabelScan(
            variable,
            labelName,
            Seq(labelPredicate),
            hint,
            qg.argumentIds,
            providedOrder,
            context
          )
        }
      case _ =>
        None
    }
  }
}
