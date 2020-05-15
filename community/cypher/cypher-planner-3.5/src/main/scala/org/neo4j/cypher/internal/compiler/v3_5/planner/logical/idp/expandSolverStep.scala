/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.PatternExpressionSolver
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.expressions.{Ands, Expression, LogicalVariable}
import org.neo4j.cypher.internal.v3_5.logical.plans.{Argument, ExpandAll, ExpandInto, LogicalLeafPlan, LogicalPlan}

case class expandSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp.expandSolverStep._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan], context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for (patternId <- goal.iterator;
           pattern <- registry.lookup(patternId);
           plan <- table(goal - patternId)) yield {
        if (plan.availableSymbols.contains(pattern.name))
          Iterator(
            planSingleProjectEndpoints(pattern, plan, context)
          )
        else
          Iterator(
            planSinglePatternSide(qg, pattern, plan, pattern.left, context),
            planSinglePatternSide(qg, pattern, plan, pattern.right, context)
          ).flatten
      }

    result.flatten
  }
}

object expandSolverStep {

  def planSingleProjectEndpoints(patternRel: PatternRelationship, plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val (start, end) = patternRel.inOrder
    val isStartInScope = plan.availableSymbols(start)
    val isEndInScope = plan.availableSymbols(end)
    context.logicalPlanProducer.planEndpointProjection(plan, start, isStartInScope, end, isEndInScope, patternRel, context)
  }

  def planSinglePatternSide(qg: QueryGraph,
                            patternRel: PatternRelationship,
                            sourcePlan: LogicalPlan,
                            nodeId: String,
                            context: LogicalPlanningContext): Option[LogicalPlan] = {
    val availableSymbols = sourcePlan.availableSymbols

    if (availableSymbols(nodeId)) {
      Some(produceLogicalPlan(qg, patternRel, sourcePlan, nodeId, availableSymbols, context))
    } else {
      None
    }
  }

  private def produceLogicalPlan(qg: QueryGraph,
                                 patternRel: PatternRelationship,
                                 sourcePlan: LogicalPlan,
                                 nodeId: String,
                                 availableSymbols: Set[String],
                                 context: LogicalPlanningContext): LogicalPlan = {
    val dir = patternRel.directionRelativeTo(nodeId)
    val otherSide = patternRel.otherSide(nodeId)
    val overlapping = availableSymbols.contains(otherSide)
    val mode = if (overlapping) ExpandInto else ExpandAll

    patternRel.length match {
      case SimplePatternLength =>
        context.logicalPlanProducer.planSimpleExpand(sourcePlan, nodeId, dir, otherSide, patternRel, mode, context)

      case _: VarPatternLength =>
        val availablePredicates: Seq[Expression] =
          qg.selections.predicatesGiven(availableSymbols + patternRel.name)
        val tempNode = patternRel.name + "_NODES"
        val tempRelationship = patternRel.name + "_RELS"
        val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], legacyPredicates: Seq[(LogicalVariable,Expression)], solvedPredicates: Seq[Expression]) =
          extractPredicates(
            availablePredicates,
            originalRelationshipName = patternRel.name,
            tempRelationship = tempRelationship,
            tempNode = tempNode,
            originalNodeName = nodeId)
        val nodePredicate = Ands.create(nodePredicates.toSet)
        val relationshipPredicate = Ands.create(relationshipPredicates.toSet)

        val (rewrittenSource, rewrittenPredicates) =
          PatternExpressionSolver().apply(sourcePlan, expressions = Seq(nodePredicate,  relationshipPredicate) ++ legacyPredicates.map(_._2), interestingOrder = InterestingOrder.empty, context)
        val rewrittenNodePredicate :: rewrittenRelationshipPredicate :: rewrittenLegacyPredicateExpressions = rewrittenPredicates.toList
        val rewrittenLegacyPredicates = legacyPredicates.map(_._1).zip(rewrittenLegacyPredicateExpressions)

        context.logicalPlanProducer.planVarExpand(
          source = rewrittenSource,
          from = nodeId,
          dir = dir,
          to = otherSide,
          pattern = patternRel,
          temporaryNode = tempNode,
          temporaryRelationship = tempRelationship,
          relationshipPredicate = rewrittenRelationshipPredicate,
          nodePredicate = rewrittenNodePredicate,
          solvedPredicates = solvedPredicates,
          mode = mode,
          legacyPredicates = rewrittenLegacyPredicates,
          context = context)
    }
  }
}
