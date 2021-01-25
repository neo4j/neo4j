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

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait OptionalSolver {
  def apply(qg: QueryGraph, lp: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Iterator[LogicalPlan]
}

case object applyOptional extends OptionalSolver {
  override def apply(optionalQg: QueryGraph, lhs: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val innerContext: LogicalPlanningContext = context.withUpdatedCardinalityInformation(lhs)
    val inner = context.strategy.plan(optionalQg, interestingOrder, innerContext)
    inner.allResults.map { inner =>
      val rhs = context.logicalPlanProducer.planOptional(inner, lhs.availableSymbols, innerContext)
      val applied = context.logicalPlanProducer.planApply(lhs, rhs, context)

      // Often the Apply can be rewritten into an OptionalExpand. We want to do that before cost estimating against the hash joins, otherwise that
      // is not a fair comparison (as they cannot be rewritten to something cheaper).
      unnestOptional(applied).asInstanceOf[LogicalPlan]
    }
  }
}

case object outerHashJoin extends OptionalSolver {
  override def apply(optionalQg: QueryGraph, side1Plan: LogicalPlan, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val joinNodes = optionalQg.argumentIds

    if (joinNodes.nonEmpty &&
      joinNodes.forall(side1Plan.availableSymbols) &&
      joinNodes.forall(optionalQg.patternNodes)) {

      // If side1 is just an Argument, any Apply above this will get written out so the incoming cardinality should be 1
      // This will be the case as [AssumeIndependenceQueryGraphCardinalityModel] will always use a cardinality of 1 if there are no
      // arguments and we delete the arguments below.
      // If not, then we're probably under an apply that will stay, so we need to force the cardinality to be multiplied by the incoming
      // cardinality.
      val side2Context = if (!side1Plan.isInstanceOf[Argument]) context.copy(input = context.input.copy(alwaysMultiply = true)) else context
      val solvedHints = optionalQg.joinHints.filter { hint =>
        val hintVariables = hint.variables.map(_.name).toSet
        hintVariables.subsetOf(joinNodes)
      }
      val rhsQG = optionalQg.withoutArguments().withoutHints(solvedHints.map(_.asInstanceOf[Hint]))

      val BestResults(side2Plan, side2SortedPlan) = context.strategy.plan(rhsQG, interestingOrder, side2Context)

      Iterator(
        leftOuterJoin(context, joinNodes, side1Plan, side2Plan, solvedHints),
        rightOuterJoin(context, joinNodes, side1Plan, side2Plan, solvedHints)
      ) ++ side2SortedPlan.map(leftOuterJoin(context, joinNodes, side1Plan, _, solvedHints))
      // For the rightOuterJoin, we do not need to consider side2SortedPlan,
      // since that ordering will get destroyed by the join anyway.
    } else {
      Iterator.empty
    }
  }

  private def leftOuterJoin(context: LogicalPlanningContext, joinNodes: Set[String], lhs: LogicalPlan, rhs: LogicalPlan, solvedHints: Set[UsingJoinHint]): LogicalPlan =
    context.logicalPlanProducer.planLeftOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)

  private def rightOuterJoin(context: LogicalPlanningContext, joinNodes: Set[String], rhs: LogicalPlan, lhs: LogicalPlan, solvedHints: Set[UsingJoinHint]): LogicalPlan =
    context.logicalPlanProducer.planRightOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)
}
