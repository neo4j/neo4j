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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.mutable

case class intersectionLabelScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    if (!context.settings.planningIntersectionScansEnabled) {
      Set.empty
    } else {
      context.staticComponents.planContext.nodeTokenIndex match {
        case Some(nodeTokenIndex) if nodeTokenIndex.orderCapability == BOTH =>
          // Combine for example HasLabels(n, Seq(A)), HasLabels(n, Seq(B)) to n -> Set(A, B)
          val combined: Map[Variable, Set[LabelName]] = {
            qg.selections.flatPredicatesSet.foldLeft(Map.empty[Variable, Set[LabelName]]) {
              case (acc, current) => current match {
                  case HasLabels(variable: Variable, labels)
                    if !skipIDs.contains(variable) && (qg.patternNodes(variable) && !qg.argumentIds(variable)) =>
                    val newValue = acc.get(variable).map(current => (current ++ labels)).getOrElse(labels.toSet)
                    acc + (variable -> newValue)
                  case _ => acc
                }
            }
          }
          // We only create one plan with the intersection of all labels, we could change this to generate all combinations, e.g.
          // given labels A, B and C
          // - (A,B,C)
          // - (A,B)
          // - (B,C)
          // - (A, C)
          // and in that way create more flexibility for the planner to plan things like
          //
          //   .nodeHashJoin("x")
          //  .|.intersectionNodeByLabelsScan("n", Seq("B", "C"))
          //  .nodeUniqueIndexSeek("n:A(prop = 42)")
          //
          // Will leave this as a future potential improvement.
          val results = mutable.HashSet.empty[LogicalPlan]
          combined.foreach {
            case (variable, labels) if labels.size > 1 =>
              val providedOrder = ResultOrdering.providedOrderForLabelScan(
                interestingOrderConfig.orderToSolve,
                variable,
                nodeTokenIndex.orderCapability,
                context.providedOrderFactory
              )
              val hints = qg.hints.collect {
                case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(name)) if labels.exists(_.name == name) => hint
              }
              results += context.staticComponents.logicalPlanProducer.planIntersectNodeByLabelsScan(
                variable,
                labels.toSeq,
                Seq(HasLabels(variable, labels.toSeq)(InputPosition.NONE)),
                hints.toSeq,
                qg.argumentIds,
                providedOrder,
                context
              )
            case _ => // do nothing
          }
          results.toSet

        case _ => Set.empty
      }
    }
  }
}
