/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LogicalPlanContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{Projection, Aggregation, QueryPlan}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{IsAggregate, Identifier, Expression}
import org.neo4j.cypher.internal.compiler.v2_1.{bottomUp, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.AggregationNameGenerator

object aggregation extends PlanTransformer {
  def apply(input: QueryPlan)(implicit context: LogicalPlanContext): QueryPlan = {
    val qg = context.queryGraph

    qg.groupingKey match {
      case None =>
        input

      case Some(keys) =>
        val (renamedProjections, renamedAggregations) = aggregationAliaser(qg.aggregatingProjections)
        val groupingProjections = qg.projections.filterKeys(keys.contains)
        val finalProjections = groupingProjections.map(p => p._1 -> Identifier(p._1)(p._2.position))

        val outputPlan =
          Projection(
            Aggregation(input.plan, groupingProjections, renamedAggregations),
            finalProjections ++ renamedProjections
          )

        val outputGraph = input.solved
          .withAggregatingProjections(qg.aggregatingProjections)
          .withProjections(groupingProjections)

        QueryPlan( outputPlan, outputGraph )
    }
  }
}


object aggregationAliaser {
  def apply(projections: Map[String, Expression]): (Map[String, Expression], Map[String, Expression]) = {
    projections.foldLeft((projections, Map.empty[String, Expression])) {
      case ((curProjections, curAggregations), (k, expr)) =>
        val aliases = findAliases(expr)
        val newExpr = applyAliases(expr, aliases)

        val newProjections = curProjections + (k -> newExpr)
        val newAggregations = curAggregations ++ aliases
        (newProjections, newAggregations)
    }
  }

  private def findAliases(expr: Expression): Map[String, Expression] =
    expr.treeFold(Map.empty[String, Expression]) {
      case IsAggregate(aggrExpr) =>
        (aliases, children) => aliases + (AggregationNameGenerator.name(aggrExpr.position) -> aggrExpr)
    }


  private def applyAliases(expr: Expression, aliases: Map[String, Expression]) = {
    val reversedAliases = aliases.map(_.swap)
    val rewriter = Rewriter.lift {
      case expr: Expression =>
        reversedAliases.get(expr) match {
          case Some(name) => Identifier(name)(expr.position)
          case _          => expr
        }
    }
    expr.typedRewrite[Expression](bottomUp(rewriter))
  }
}
