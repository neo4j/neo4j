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

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.AbstractNodeIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.isAllowedByRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.NodeIndexMatch
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.macros.AssertMacros

/*
 * Plan the following type of plan
 *
 *  - as := AssertSame
 *  - ui := NodeUniqueIndexSeek
 *
 *       (as)
 *       /  \
 *    (as) (ui3)
 *    /  \
 * (ui1) (ui2)
 */
object mergeNodeUniqueIndexSeekLeafPlanner
    extends NodeIndexLeafPlanner(Seq(nodeSingleUniqueIndexSeekPlanProvider), LeafPlanRestrictions.NoRestrictions) {

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def solvedQueryGraph(plan: LogicalPlan): QueryGraph =
      context.staticComponents.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph

    val resultPlans: Set[LogicalPlan] = super.apply(qg, interestingOrderConfig, context)

    val grouped: Map[LogicalVariable, Set[LogicalPlan]] = resultPlans.groupBy { p =>
      val solvedQG = solvedQueryGraph(p)
      val patternNodes = solvedQG.patternNodes

      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        patternNodes.size == 1,
        "Node unique index plan solved more than one pattern node."
      )
      patternNodes.head
    }

    grouped.map {
      case (id, plans) =>
        plans.reduce[LogicalPlan] {
          case (p1, p2) => context.staticComponents.logicalPlanProducer.planAssertSameNode(id, p1, p2, context)
        }
    }.toSet
  }
}

object nodeSingleUniqueIndexSeekPlanProvider extends AbstractNodeIndexSeekPlanProvider {

  override def createPlans(
    indexMatches: Set[NodeIndexMatch],
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch.propertyPredicates, restrictions) && indexMatch.indexDescriptor.isUnique
    solution <- createSolution(indexMatch, hints, argumentIds, context)
    if isSingleUniqueQuery(solution.valueExpr)
  } yield constructPlan(solution, context)

  private def isSingleUniqueQuery(valueExpr: QueryExpression[_]): Boolean = valueExpr match {
    case _: SingleQueryExpression[_]    => true
    case c: CompositeQueryExpression[_] => c.inner.forall(isSingleUniqueQuery)
    case _                              => false
  }

  override def constructPlan(solution: Solution, context: LogicalPlanningContext): LogicalPlan = {
    context.staticComponents.logicalPlanProducer.planNodeUniqueIndexSeek(
      solution.variable,
      solution.label,
      solution.properties,
      solution.valueExpr,
      solution.solvedPredicates,
      solution.hint,
      solution.argumentIds,
      solution.providedOrder,
      solution.indexOrder,
      context,
      solution.indexType
    )
  }
}
