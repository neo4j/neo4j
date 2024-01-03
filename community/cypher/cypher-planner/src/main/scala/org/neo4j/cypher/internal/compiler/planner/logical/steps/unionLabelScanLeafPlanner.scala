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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability

import scala.collection.immutable.ListSet

case class unionLabelScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  private def variableIfAllEqualHasLabels(expressions: ListSet[Expression]): Option[(Variable, Seq[LabelName])] = {
    val maybeSingleVar = expressions.headOption
      .collect {
        case HasLabels(variable: Variable, _) => variable
      }
      .filter(variable =>
        expressions.tail.forall {
          case HasLabels(`variable`, _) => true
          case _                        => false
        }
      )

    maybeSingleVar match {
      case Some(singleVar) =>
        Some((
          singleVar,
          expressions.collect {
            case HasLabels(_, Seq(label)) => label
          }.toSeq
        ))
      case None => None
    }
  }

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    qg.selections.flatPredicatesSet.flatMap {
      case ors @ Ors(exprs) =>
        variableIfAllEqualHasLabels(exprs).collect {
          case (variable, labels)
            if !skipIDs.contains(variable) &&
              qg.patternNodes(variable) &&
              !qg.argumentIds(variable) =>
            context.staticComponents.planContext.nodeTokenIndex.flatMap { nodeTokenIndex =>
              // UnionNodeByLabelScan relies on ordering, so we can only use this plan if the nodeTokenIndex is ordered.
              if (nodeTokenIndex.orderCapability == IndexOrderCapability.BOTH) {
                val hints = qg.hints.toSeq.collect {
                  case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(labelName))
                    if labels.map(_.name).contains(labelName) => hint
                }

                val providedOrder = ResultOrdering.providedOrderForLabelScan(
                  interestingOrderConfig.orderToSolve,
                  variable,
                  nodeTokenIndex.orderCapability,
                  context.providedOrderFactory
                )

                val plan = context.staticComponents.logicalPlanProducer.planUnionNodeByLabelsScan(
                  variable,
                  labels,
                  Seq(ors),
                  hints,
                  qg.argumentIds,
                  providedOrder,
                  context
                )
                Some(plan)
              } else {
                None
              }
            }
        }.flatten
      case _ =>
        None
    }
  }
}
