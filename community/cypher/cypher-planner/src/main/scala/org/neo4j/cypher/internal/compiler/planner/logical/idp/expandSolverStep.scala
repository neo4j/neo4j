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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.equalsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.VariablePredicate

import scala.collection.immutable.ListSet

case class expandSolverStep(qg: QueryGraph)
    extends IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext] {

  override def apply(
    registry: IdRegistry[NodeConnection],
    goal: Goal,
    table: IDPCache[LogicalPlan],
    context: LogicalPlanningContext
  ): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for {
        patternId <- goal.bitSet.iterator
        plan <- table(Goal(goal.bitSet - patternId)).iterator
        pattern <- registry.lookup(patternId)
      } yield {
        pattern match {
          case relationship: PatternRelationship if plan.availableSymbols.contains(relationship.name) =>
            Iterator(
              // we do not project endpoints for quantified path patterns
              planSingleProjectEndpoints(relationship, plan, context)
            )
          case _ =>
            Iterator(
              planSinglePatternSide(qg, pattern, plan, pattern.left, context),
              planSinglePatternSide(qg, pattern, plan, pattern.right, context)
            ).flatten
        }
      }

    result.flatten
  }
}

object expandSolverStep {

  def planSingleProjectEndpoints(
    patternRel: PatternRelationship,
    plan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (start, end) = patternRel.inOrder
    val isStartInScope = plan.availableSymbols(start)
    val isEndInScope = plan.availableSymbols(end)
    context.logicalPlanProducer.planProjectEndpoints(
      plan,
      start,
      isStartInScope,
      end,
      isEndInScope,
      patternRel,
      context
    )
  }

  def planSinglePatternSide(
    qg: QueryGraph,
    patternRel: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: String,
    context: LogicalPlanningContext
  ): Option[LogicalPlan] = {
    val availableSymbols = sourcePlan.availableSymbols

    if (availableSymbols(nodeId)) {
      Some(produceLogicalPlan(qg, patternRel, sourcePlan, nodeId, availableSymbols, context))
    } else {
      None
    }
  }

  private def produceLogicalPlan(
    qg: QueryGraph,
    patternRel: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    patternRel match {
      case rel: PatternRelationship   => produceLogicalPlan(qg, rel, sourcePlan, nodeId, availableSymbols, context)
      case qpp: QuantifiedPathPattern => produceLogicalPlan(qpp, sourcePlan, nodeId, availableSymbols, context)
    }
  }

  private def produceLogicalPlan(
    qg: QueryGraph,
    patternRel: PatternRelationship,
    sourcePlan: LogicalPlan,
    nodeId: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val dir = patternRel.directionRelativeTo(nodeId)
    val otherSide = patternRel.otherSide(nodeId)
    val overlapping = availableSymbols.contains(otherSide)
    val mode = if (overlapping) ExpandInto else ExpandAll

    patternRel.length match {
      case SimplePatternLength =>
        context.logicalPlanProducer.planSimpleExpand(sourcePlan, nodeId, dir, otherSide, patternRel, mode, context)

      case _: VarPatternLength =>
        val availablePredicates: collection.Seq[Expression] =
          qg.selections.predicatesGiven(availableSymbols + patternRel.name + otherSide)
        val (
          nodePredicates: ListSet[VariablePredicate],
          relationshipPredicates: ListSet[VariablePredicate],
          solvedPredicates: ListSet[Expression]
        ) =
          extractPredicates(
            availablePredicates,
            originalRelationshipName = patternRel.name,
            originalNodeName = nodeId,
            targetNodeName = otherSide
          )

        context.logicalPlanProducer.planVarExpand(
          source = sourcePlan,
          from = nodeId,
          dir = dir,
          to = otherSide,
          pattern = patternRel,
          nodePredicates = nodePredicates,
          relationshipPredicates = relationshipPredicates,
          solvedPredicates = solvedPredicates,
          mode = mode,
          context = context
        )
    }
  }

  private def produceLogicalPlan(
    quantifiedPathPattern: QuantifiedPathPattern,
    sourcePlan: LogicalPlan,
    startNode: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val leftIsStart = startNode == quantifiedPathPattern.left
    val startBinding = if (leftIsStart) quantifiedPathPattern.leftBinding else quantifiedPathPattern.rightBinding
    val endBinding = if (leftIsStart) quantifiedPathPattern.rightBinding else quantifiedPathPattern.leftBinding

    val overlapping = availableSymbols.contains(quantifiedPathPattern.otherSide(startNode))
    val (modifiedEndBinding, maybeHiddenFilter) =
      if (overlapping) {
        val newName = context.anonymousVariableNameGenerator.nextName
        val hiddenFilter = equalsPredicate(newName, endBinding.outer)

        (endBinding.copy(outer = newName), Some(hiddenFilter))
      } else {
        (endBinding, None)
      }

    context.logicalPlanProducer.planTrail(
      source = sourcePlan,
      pattern = quantifiedPathPattern,
      startBinding = startBinding,
      endBinding = modifiedEndBinding,
      maybeHiddenFilter = maybeHiddenFilter,
      context = context
    )
  }
}
