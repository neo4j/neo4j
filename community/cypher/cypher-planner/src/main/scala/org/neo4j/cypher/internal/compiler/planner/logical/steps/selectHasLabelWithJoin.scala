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

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections.containsExistsSubquery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case object selectHasLabelWithJoin extends SelectionCandidateGenerator {

  override def apply(
    input: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate] = {
    context.staticComponents.planContext.nodeTokenIndex match {
      case None => Iterator.empty
      case Some(nodeTokenIndex) =>
        unsolvedPredicates.iterator.filterNot(containsExistsSubquery).collect {
          case s @ HasLabels(variable: Variable, Seq(labelName))
            if queryGraph.patternNodes.contains(variable) && !queryGraph.argumentIds.contains(variable) =>
            val providedOrder = ResultOrdering.providedOrderForLabelScan(
              interestingOrderConfig.orderToSolve,
              variable,
              nodeTokenIndex.orderCapability,
              context.providedOrderFactory
            )
            val labelScan = context.staticComponents.logicalPlanProducer.planNodeByLabelScan(
              variable,
              labelName,
              Seq(s),
              None,
              queryGraph.argumentIds,
              providedOrder,
              context
            )
            val plan =
              context.staticComponents.logicalPlanProducer.planNodeHashJoin(
                Set(variable),
                input,
                labelScan,
                Set.empty,
                context
              )
            SelectionCandidate(plan, Set(s))
        }
    }
  }
}
