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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PipeBuilder, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStep._
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.{ApplyRewriter, RewriterStepSequencer}

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class Planner(monitors: Monitors,
                   metricsFactory: MetricsFactory,
                   monitor: PlanningMonitor,
                   tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
                   maybeExecutionPlanBuilder: Option[PipeExecutionPlanBuilder] = None,
                   strategy: PlanningStrategy = new QueryPlanningStrategy(),
                   queryGraphSolver: QueryGraphSolver = new GreedyQueryGraphSolver()) extends PipeBuilder {

  val executionPlanBuilder: PipeExecutionPlanBuilder = maybeExecutionPlanBuilder.getOrElse(new PipeExecutionPlanBuilder(monitors))

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    Planner.rewriteStatement(inputQuery.statement, inputQuery.scopeTree) match {
      case ast: Query =>
        monitor.startedPlanning(inputQuery.queryText)
        val (logicalPlan, pipeBuildContext) = produceLogicalPlan(ast, inputQuery.semanticTable)(planContext)
        monitor.foundPlan(inputQuery.queryText, logicalPlan)
        val result = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, planContext)
        monitor.successfulPlanning(inputQuery.queryText, result)
        result

      case _ =>
        throw new CantHandleQueryException
    }
  }

  def produceLogicalPlan(ast: Query, semanticTable: SemanticTable)(planContext: PlanContext): (LogicalPlan, PipeExecutionBuilderContext) = {
    tokenResolver.resolve(ast)(semanticTable, planContext)
    val unionQuery = ast.asUnionQuery

    val metrics = metricsFactory.newMetrics(planContext.statistics, semanticTable)

    val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver)
    val plan = strategy.plan(unionQuery)(context)

    val pipeBuildContext = PipeExecutionBuilderContext((e: PatternExpression) => {
      val expressionQueryGraph = e.asQueryGraph
      val argLeafPlan = Some(planQueryArgumentRow(expressionQueryGraph))
      val LogicalPlan = queryGraphSolver.plan(expressionQueryGraph)(context, argLeafPlan)
      LogicalPlan
    }, metrics.cardinality, semanticTable)


    (plan, pipeBuildContext)
  }
}

object Planner {
  import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStep._

  def getRewriter(scopeTree: Scope): Rewriter = {
    val namespacedIdentifiers = namespaceIdentifiers(scopeTree)

    RewriterStepSequencer.newDefault("Planner")(
      ApplyRewriter("namespaceIdentifiers", namespacedIdentifiers),

      rewriteEqualityToInCollection,
      splitInCollectionsToIsolateConstants,
      CNFNormalizer,
      collapseInCollectionsContainingConstants,
      namePatternPredicates,
      nameUpdatingClauses,
      projectNamedPaths,
      projectFreshSortExpressions,
      inlineProjections
    )
  }

  def rewriteStatement(statement: Statement, scopeTree: Scope) = {
    statement.endoRewrite(getRewriter(scopeTree))
  }
}

trait PlanningMonitor {
  def startedPlanning(q: String)
  def foundPlan(q: String, p: LogicalPlan)
  def successfulPlanning(q: String, p: PipeInfo)
}
