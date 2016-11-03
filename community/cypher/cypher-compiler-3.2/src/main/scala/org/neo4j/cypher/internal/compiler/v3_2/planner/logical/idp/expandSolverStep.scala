/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.ast._

case class expandSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp.expandSolverStep._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for (patternId <- goal.iterator;
           pattern <- registry.lookup(patternId);
           plan <- table(goal - patternId)
      ) yield {
        if (plan.availableSymbols.contains(pattern.name))
          Iterator.apply(
            planSingleProjectEndpoints(pattern, plan)
          )
        else
          Iterator(
            planSinglePatternSide(qg, pattern, plan, pattern.left),
            planSinglePatternSide(qg, pattern, plan, pattern.right)
          ).flatten
      }

    // This should be (and is) lazy
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

  def planSinglePatternSide(qg: QueryGraph, patternRel: PatternRelationship, plan: LogicalPlan, nodeId: IdName)
                           (implicit context: LogicalPlanningContext): Option[LogicalPlan] = {
    val availableSymbols = plan.availableSymbols
    if (availableSymbols(nodeId)) {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          Some(context.logicalPlanProducer.planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode))

        case length: VarPatternLength =>
          val availablePredicates = qg.selections.predicatesGiven(availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            //MATCH ()-[r* {prop:1337}]->()
            case all@AllIterablePredicate(FilterScope(variable, Some(innerPredicate)), relId@Variable(patternRel.name.name))
              if variable == relId || !innerPredicate.dependencies(relId) =>
              (variable, innerPredicate) -> all
            //MATCH p = ... WHERE all(n in nodes(p)... or all(r in relationships(p)
            case all@AllIterablePredicate(FilterScope(variable, Some(innerPredicate)),
                                          FunctionInvocation(_, FunctionName(fname), false,
                                                             Seq(PathExpression(
                                                             NodePathStep(startNode, MultiRelationshipPathStep(rel, _, NilPathStep) ))) ))
              if (fname  == "nodes" || fname == "relationships") && startNode.name == nodeId.name && rel.name == patternRel.name.name =>
              (variable, innerPredicate) -> all

            //MATCH p = ... WHERE all(n in nodes(p)... or all(r in relationships(p)
            case none@NoneIterablePredicate(FilterScope(variable, Some(innerPredicate)),
                                            FunctionInvocation(_, FunctionName(fname), false,
                                                               Seq(PathExpression(
                                                             NodePathStep(startNode, MultiRelationshipPathStep(rel, _, NilPathStep) ))) ))
              if (fname  == "nodes" || fname == "relationships") && startNode.name == nodeId.name && rel.name == patternRel.name.name =>
              (variable, Not(innerPredicate)(innerPredicate.position)) -> none
          }.unzip
          Some(context.logicalPlanProducer.planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, mode))
      }
    } else {
      None
    }
  }
}
