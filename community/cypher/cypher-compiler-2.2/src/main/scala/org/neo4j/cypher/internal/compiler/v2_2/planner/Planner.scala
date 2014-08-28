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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeBuilder
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeInfo

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class Planner(monitors: Monitors,
                   metricsFactory: MetricsFactory,
                   monitor: PlanningMonitor,
                   tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
                   maybeExecutionPlanBuilder: Option[PipeExecutionPlanBuilder] = None,
                   strategy: PlanningStrategy = new QueryPlanningStrategy(),
                   queryGraphSolver: QueryGraphSolver = new GreedyQueryGraphSolver()) extends PipeBuilder {

  val executionPlanBuilder:PipeExecutionPlanBuilder = maybeExecutionPlanBuilder.getOrElse(new PipeExecutionPlanBuilder(monitors))

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo =
    producePlan(inputQuery.statement, inputQuery.semanticTable, inputQuery.queryText)(planContext)

  private def producePlan(statement: Statement, semanticTable: SemanticTable, query: String)(planContext: PlanContext): PipeInfo = {
    Planner.rewriteStatement(statement) match {
      case ast: Query =>
        monitor.startedPlanning(query)
        val (logicalPlan, pipeBuildContext) = produceQueryPlan(ast, semanticTable)(planContext)
        monitor.foundPlan(query, logicalPlan)
        val result = executionPlanBuilder.build(logicalPlan)(pipeBuildContext)
        monitor.successfulPlanning(query, result)
        result

      case _ =>
        throw new CantHandleQueryException
    }
  }

  def produceQueryPlan(ast: Query, semanticTable: SemanticTable)(planContext: PlanContext): (LogicalPlan, PipeExecutionBuilderContext) = {
    tokenResolver.resolve(ast)(semanticTable, planContext)
    val unionQuery = ast.asUnionQuery

    val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)

    val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
    val plan = strategy.plan(unionQuery)(context)

    val pipeBuildContext = PipeExecutionBuilderContext((e: PatternExpression) => {
      val expressionQueryGraph = e.asQueryGraph
      val argLeafPlan = Some(planQueryArgumentRow(expressionQueryGraph))
      val queryPlan = queryGraphSolver.plan(expressionQueryGraph)(context, argLeafPlan)
      queryPlan.plan
    })


    (plan, pipeBuildContext)
  }
}

object Planner {
  val rewriter = inSequence(
    rewriteEqualityToInCollection,
    splitInCollectionsToIsolateConstants,
    CNFNormalizer,
    collapseInCollectionsContainingConstants,
    nameVarLengthRelationships,
    namePatternPredicates,
    projectNamedPaths,
    projectFreshSortExpressions,
    inlineProjections
  )

  def rewriteStatement(statement: Statement) = statement.endoRewrite(rewriter)
}

trait PlanningMonitor {
  def startedPlanning(q: String)
  def foundPlan(q: String, p: LogicalPlan)
  def successfulPlanning(q: String, p: PipeInfo)
}
