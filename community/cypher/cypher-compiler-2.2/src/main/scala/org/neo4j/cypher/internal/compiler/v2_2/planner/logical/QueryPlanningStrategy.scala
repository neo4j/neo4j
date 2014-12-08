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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.QueryPlanningStrategy.patternExpressionRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps._

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                            rewriter: Rewriter = LogicalPlanRewriter)
  extends PlanningStrategy {

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan = unionQuery match {
    case UnionQuery(queries, distinct) =>
      val plan = planQuery(queries, distinct)
      plan.endoRewrite(rewriter)

    case _ =>
      throw new CantHandleQueryException
  }

  private def planQuery(queries: Seq[PlannerQuery], distinct: Boolean)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None) = {
    val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p))
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => planUnion(p1, p2)
    }

    if (distinct)
      planDistinct(unionPlan)
    else
      unionPlan
  }

  protected def planSingleQuery(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan = {
    val firstPart = planPart(query, leafPlan)
    val projectedFirstPart = planEventHorizon(query, firstPart)
    val finalPlan = planWithTail(projectedFirstPart, query.tail)
    verifyBestPlan(finalPlan, query)
  }

  private def planWithTail(pred: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = remaining match {
    case Some(query) =>
      val lhs = pred
      val lhsContext = context.recurse(lhs)
      val rhs = planPart(query, Some(planQueryArgumentRow(query.graph)))(lhsContext)
      val applyPlan = planTailApply(lhs, rhs)

      val applyContext = lhsContext.recurse(applyPlan)
      val projectedPlan = planEventHorizon(query, applyPlan)(applyContext)

      val projectedContext = applyContext.recurse(projectedPlan)
      val completePlan = planNestedPlanExpressions(projectedPlan)(projectedContext)

      // planning nested expressions doesn't change outer cardinality
      planWithTail(completePlan, query.tail)(projectedContext)

    case None =>
      pred
  }

  private def planPart(query: PlannerQuery, leafPlan: Option[LogicalPlan])(implicit context: LogicalPlanningContext): LogicalPlan = {
    context.strategy.plan(query.graph)(context, leafPlan)
  }

  private def planEventHorizon(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = config.applySelections(plan, query.graph)
    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection)
        sortSkipAndLimit(aggregationPlan, query)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        projection(sortedAndLimited, queryProjection.projections, intermediate = query.tail.isDefined)

      case UnwindProjection(identifier, expression) =>
        planUnwind(plan, identifier, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }

  private def planNestedPlanExpressions(plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    case object planExpressionRewriter extends Rewriter {
      override def apply(in: AnyRef): AnyRef = in match {
        case plan: LogicalPlan =>
          plan.mapExpressions {
            case (arguments, expression) =>
              val exprScopes = expression.inputs.map { case (k, v) => k -> v.map(IdName.fromIdentifier) }
              val exprScopeMap = IdentityMap(exprScopes: _*)
              expression.rewrite(patternExpressionRewriter(arguments, exprScopeMap)).asInstanceOf[Expression]
          }
      }
    }

    plan.endoRewrite(planExpressionRewriter)
  }
}

object QueryPlanningStrategy {

  def planPatternExpression(planArguments: Set[IdName], expr: PatternExpression)
                           (implicit context: LogicalPlanningContext): (LogicalPlan, PatternExpression) = {
    val dependencies = expr.dependencies.map(IdName.fromIdentifier)
    val qgArguments = planArguments intersect dependencies
    val (namedExpr, namedMap) = PatternExpressionPatternElementNamer(expr)
    val qg = namedExpr.asQueryGraph.withArgumentIds(qgArguments)

    val argLeafPlan = Some(planQueryArgumentRow(qg))
    val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
    val patternPlanningContext = context.forExpressionPlanning(namedNodes, namedRels)
    val queryGraphSolver = patternPlanningContext.strategy
    val plan = queryGraphSolver.plan(qg)(patternPlanningContext, argLeafPlan)
    (plan, namedExpr)
  }

  private case class patternExpressionRewriter(planArguments: Set[IdName], exprArguments: IdentityMap[Expression, Set[IdName]])
                                              (implicit context: LogicalPlanningContext) extends Rewriter {
    val instance = Rewriter.lift {
      case expr @ PatternExpression(pattern) =>
        val (plan, namedExpr) =  planPatternExpression(planArguments ++ exprArguments(expr), expr)
        NestedPlanExpression(plan, namedExpr)(namedExpr.position)
    }

    def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)
  }
}
