/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.v3_3.logical.plans.{ExpandAll, ExpandInto, LogicalPlan}

case class expandSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp.expandSolverStep._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for (patternId <- goal.iterator;
           pattern <- registry.lookup(patternId);
           plan <- table(goal - patternId)) yield {
        if (plan.availableSymbols.contains(pattern.name))
          Iterator(
            planSingleProjectEndpoints(pattern, plan)
          )
        else
          Iterator(
            planSinglePatternSide(qg, pattern, plan, pattern.left),
            planSinglePatternSide(qg, pattern, plan, pattern.right)
          ).flatten
      }

    result.flatten
  }
}

object expandSolverStep {

  def planSingleProjectEndpoints(patternRel: PatternRelationship, plan: LogicalPlan)
                                (implicit context: LogicalPlanningContext): LogicalPlan = {
    val (start, end) = patternRel.inOrder
    val isStartInScope = plan.availableSymbols(start)
    val isEndInScope = plan.availableSymbols(end)
    context.logicalPlanProducer.planEndpointProjection(plan, start, isStartInScope, end, isEndInScope, patternRel)
  }

  def planSinglePatternSide(qg: QueryGraph, patternRel: PatternRelationship, sourcePlan: LogicalPlan, nodeId: String)
                           (implicit context: LogicalPlanningContext): Option[LogicalPlan] = {
    val availableSymbols = sourcePlan.availableSymbols
    if (availableSymbols(nodeId)) {
      Some(produceLogicalPlan(qg, patternRel, sourcePlan, nodeId, availableSymbols))
    } else {
      None
    }
  }

  private def produceLogicalPlan(qg: QueryGraph,
                                 patternRel: PatternRelationship,
                                 sourcePlan: LogicalPlan,
                                 nodeId: String,
                                 availableSymbols: Set[String])
                                (implicit context: LogicalPlanningContext): LogicalPlan = {
    val dir = patternRel.directionRelativeTo(nodeId)
    val otherSide = patternRel.otherSide(nodeId)
    val overlapping = availableSymbols.contains(otherSide)
    val mode = if (overlapping) ExpandInto else ExpandAll

    patternRel.length match {
      case SimplePatternLength =>
        context.logicalPlanProducer.planSimpleExpand(sourcePlan, nodeId, dir, otherSide, patternRel, mode)

      case _: VarPatternLength =>
        val availablePredicates: Seq[Expression] =
          qg.selections.predicatesGiven(availableSymbols + patternRel.name)
        val tempNode = patternRel.name + "_NODES"
        val tempEdge = patternRel.name + "_RELS"
        val (nodePredicates: Seq[Expression], edgePredicates: Seq[Expression], legacyPredicates: Seq[(Variable,Expression)], solvedPredicates: Seq[Expression]) =
          extractPredicates(
            availablePredicates,
            originalEdgeName = patternRel.name,
            tempEdge = tempEdge,
            tempNode = tempNode,
            originalNodeName = nodeId)
        val nodePredicate = Ands.create(nodePredicates.toSet)
        val relationshipPredicate = Ands.create(edgePredicates.toSet)

        context.logicalPlanProducer.planVarExpand(
          left = sourcePlan,
          from = nodeId,
          dir = dir,
          to = otherSide,
          pattern = patternRel,
          temporaryNode = tempNode,
          temporaryEdge = tempEdge,
          edgePredicate = relationshipPredicate,
          nodePredicate = nodePredicate,
          solvedPredicates = solvedPredicates,
          mode = mode,
          legacyPredicates = legacyPredicates)
    }
  }

}
