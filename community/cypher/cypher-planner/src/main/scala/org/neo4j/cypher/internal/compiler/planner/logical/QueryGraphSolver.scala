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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodePatternExpression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.asQueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Ref

trait QueryGraphSolver {
  def plan(queryGraph: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): BestPlans
  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): LogicalPlan
  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): LogicalPlan

  def planInnerOfExistsSubquery(planArguments: Set[String],
                                e: ExistsSubClause,
                                interestingOrderConfig: InterestingOrderConfig,
                                context: LogicalPlanningContext): LogicalPlan
}

trait PatternExpressionSolving {

  self: QueryGraphSolver =>

  def planPatternExpression(planArguments: Set[String], expr: PatternExpression, context: LogicalPlanningContext): LogicalPlan = {
    val qg = asQueryGraph(expr, planArguments, context.anonymousVariableNameGenerator)
    self.plan(qg, InterestingOrderConfig.empty, context).result
  }

  def planPatternComprehension(planArguments: Set[String], expr: PatternComprehension, context: LogicalPlanningContext): LogicalPlan = {
    val qg = asQueryGraph(expr, planArguments, context.anonymousVariableNameGenerator)
    self.plan(qg, InterestingOrderConfig.empty, context).result
  }
}

trait ExistsSubquerySolving {

  self: QueryGraphSolver =>

  private val cachedPlanInnerOfExistsSubquery = CachedFunction(doPlan _)

  override def planInnerOfExistsSubquery(planArguments: Set[String],
                                         e: ExistsSubClause,
                                         interestingOrderConfig: InterestingOrderConfig,
                                         context: LogicalPlanningContext): LogicalPlan = {
    cachedPlanInnerOfExistsSubquery(planArguments, Ref(e), interestingOrderConfig, Ref(context))
  }

  private def doPlan(planArguments: Set[String],
                     eRef: Ref[ExistsSubClause],
                     interestingOrderConfig: InterestingOrderConfig,
                     contextRef: Ref[LogicalPlanningContext]): LogicalPlan = {
    val context = contextRef.value
    val e = eRef.value
    // Creating a query graph by combining all extracted query graphs created by each entry of the patternElements
    val qg = e.patternElements.foldLeft(QueryGraph.empty) { (acc, patternElement) =>
      patternElement match {
        case elem: RelationshipChain =>
          val variableToCollectName = context.anonymousVariableNameGenerator.nextName
          val collectionName = context.anonymousVariableNameGenerator.nextName
          val patternExpr = PatternExpression(RelationshipsPattern(elem)(elem.position))(e.outerScope, variableToCollectName, collectionName)
          val qg = asQueryGraph(patternExpr, planArguments, context.anonymousVariableNameGenerator)
          acc ++ qg

        case elem: NodePattern =>
          val patternExpr = NodePatternExpression(List(elem))(elem.position)
          val qg = asQueryGraph(patternExpr, planArguments, context.anonymousVariableNameGenerator)
          acc ++ qg
      }
    }

    // Adding the predicates and known outer variables to new query graph
    val new_qg = e.optionalWhereExpression.foldLeft(qg) {
      case (acc: QueryGraph, patternExpr: Expression) =>
        val outerVariableNames = e.outerScope.map(id => id.name)
        val usedVariables: Seq[String] = patternExpr.arguments.folder
          .findAllByClass[Variable]
          .map(_.name)
          .distinct

        acc.addPredicates(outerVariableNames, patternExpr)
          .addArgumentIds(usedVariables.filter(v => outerVariableNames.contains(v)))
    }

    self.plan(new_qg, interestingOrderConfig, context).result
  }
}
