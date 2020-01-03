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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions.{Ands, Expression, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition

case class expandSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, InterestingOrder, LogicalPlan, LogicalPlanningContext] {

  import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan, InterestingOrder], context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for {patternId <- goal.iterator
           (interestingOrder, plan) <- table(goal - patternId)
           pattern <- registry.lookup(patternId)
      } yield {
        if (plan.availableSymbols.contains(pattern.name))
          Iterator(
            planSingleProjectEndpoints(pattern, plan, context)
          )
        else
          Iterator(
            planSinglePatternSide(qg, pattern, plan, pattern.left, interestingOrder, context),
            planSinglePatternSide(qg, pattern, plan, pattern.right, interestingOrder, context)
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
                            interestingOrder: InterestingOrder,
                            context: LogicalPlanningContext): Option[LogicalPlan] = {
    val availableSymbols = sourcePlan.availableSymbols

    /*
     * Method to find implicit leaf plan arguments, except explicit Argument
     */
    def leafArguments(plan:LogicalPlan): Set[String] = plan match {
      case _: Argument => Set.empty
      case p: LogicalLeafPlan => p.argumentIds
      case _ =>
        val lhs = plan.lhs.map(inner => leafArguments(inner)).toSet.flatten
        val rhs = plan.rhs.map(inner => leafArguments(inner)).toSet.flatten
        lhs ++ rhs
    }
    // Remove the leaf arguments as to not try and join disjoint plans only on arguments.
    // This reduces the solution space for the expands generator, since the joins generator should consider these cases.
    val symbols = availableSymbols -- leafArguments(sourcePlan)

    if (symbols(nodeId)) {
      Some(produceLogicalPlan(qg, patternRel, sourcePlan, nodeId, availableSymbols, interestingOrder, context))
    } else {
      None
    }
  }

  private def produceLogicalPlan(qg: QueryGraph,
                                 patternRel: PatternRelationship,
                                 sourcePlan: LogicalPlan,
                                 nodeId: String,
                                 availableSymbols: Set[String],
                                 interestingOrder: InterestingOrder,
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
          qg.selections.predicatesGiven(availableSymbols + patternRel.name + otherSide)
        val tempNode = patternRel.name + "_NODES"
        val tempRelationship = patternRel.name + "_RELS"
        val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
          extractPredicates(
            availablePredicates,
            originalRelationshipName = patternRel.name,
            tempRelationship = tempRelationship,
            tempNode = tempNode,
            originalNodeName = nodeId)

        context.logicalPlanProducer.planVarExpand(
          source = sourcePlan,
          from = nodeId,
          dir = dir,
          to = otherSide,
          pattern = patternRel,
          nodePredicate = variablePredicate(tempNode, nodePredicates),
          relationshipPredicate = variablePredicate(tempRelationship, relationshipPredicates),
          solvedPredicates = solvedPredicates,
          mode = mode,
          interestingOrder = interestingOrder,
          context = context)
    }
  }

  private def variablePredicate(tempVariableName: String, predicates: Seq[Expression]): Option[VariablePredicate] =
    if (predicates.isEmpty)
      None
    else
      Some(VariablePredicate(Variable(tempVariableName)(InputPosition.NONE), Ands.create(predicates.toSet)))

}
