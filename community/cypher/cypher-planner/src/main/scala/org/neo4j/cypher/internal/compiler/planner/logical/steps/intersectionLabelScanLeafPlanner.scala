/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.InputPosition

case class intersectionLabelScanLeafPlanner(skipIDs: Set[String]) extends LeafPlanner {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    // Combine for example HasLabels(n, Seq(A)), HasLabels(n, Seq(B)) to n -> Set(A, B)
    val combined: Map[Variable, Set[LabelName]] = {
      qg.selections.flatPredicatesSet.foldLeft(Map.empty[Variable, Set[LabelName]]) {
        case (acc, current) => current match {
            case HasLabels(variable @ Variable(varName), labels)
              if !skipIDs.contains(varName) && context.planContext.canLookupNodesByLabel && (qg.patternNodes(
                varName
              ) && !qg.argumentIds(varName)) =>
              val newValue = acc.get(variable).map(current => (current ++ labels)).getOrElse(labels.toSet)
              acc + (variable -> newValue)
            case _ => acc
          }
      }
    }

    combined.flatMap {
      case (variable, labels) =>
        val providedOrder = ResultOrdering.providedOrderForLabelScan(
          interestingOrderConfig.orderToSolve,
          variable,
          context.providedOrderFactory
        )

        // Given (n:A&B&C) we want to plan :
        // - intersectionNodeLabelScan(A,B,C)
        // - intersectionNodeLabelScan(A,B)
        // - intersectionNodeLabelScan(B,C)
        // - intersectionNodeLabelScan(A,C),
        labels.subsets.flatMap {
          case subset if subset.isEmpty || subset.size == 1 => None
          case subset =>
            val hints = qg.hints.collect {
              case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(name)) if labels.exists(_.name == name) => hint
            }
            Some(context.logicalPlanProducer.planIntersectNodeByLabelsScan(
              variable,
              subset.toSeq,
              Seq(HasLabels(variable, subset.toSeq)(InputPosition.NONE)),
              hints.toSeq,
              qg.argumentIds,
              providedOrder,
              context
            ))
        }
    }.toSet
  }
}
