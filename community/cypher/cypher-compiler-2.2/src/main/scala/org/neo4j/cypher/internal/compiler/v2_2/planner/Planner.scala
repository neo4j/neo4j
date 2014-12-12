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
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.namespaceIdentifiers.IdentifierNames
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PipeBuilder, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.{ApplyRewriter, RewriterStepSequencer}
import org.neo4j.helpers.Clock

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class Planner(monitors: Monitors,
                   metricsFactory: MetricsFactory,
                   monitor: PlanningMonitor,
                   clock: Clock,
                   tokenResolver: SimpleTokenResolver = new SimpleTokenResolver(),
                   maybeExecutionPlanBuilder: Option[PipeExecutionPlanBuilder] = None,
                   strategy: PlanningStrategy = new QueryPlanningStrategy,
                   queryGraphSolver: QueryGraphSolver =
                   new CompositeQueryGraphSolver(
                     new GreedyQueryGraphSolver(expandsOrJoins),
                     new GreedyQueryGraphSolver(expandsOnly)
                   )) extends PipeBuilder {

  val executionPlanBuilder: PipeExecutionPlanBuilder =
    maybeExecutionPlanBuilder.getOrElse(new PipeExecutionPlanBuilder(clock, monitors))

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    Planner.rewriteStatement(inputQuery.statement, inputQuery.scopeTree, inputQuery.semanticTable) match {
      case (ast: Query, rewrittenSemanticTable) =>
        monitor.startedPlanning(inputQuery.queryText)
        val (logicalPlan, pipeBuildContext) = produceLogicalPlan(ast, rewrittenSemanticTable)(planContext)
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
    val context = LogicalPlanningContext(planContext, metrics, semanticTable, queryGraphSolver, QueryGraphCardinalityInput.empty)
    val plan = strategy.plan(unionQuery)(context)
    val pipeBuildContext = PipeExecutionBuilderContext(metrics.cardinality, semanticTable)

    (plan, pipeBuildContext)
  }
}

object Planner {
  import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStep._

  def rewriteStatement(statement: Statement, scopeTree: Scope, semanticTable: SemanticTable): (Statement, SemanticTable) = {
    val namespacer = namespaceIdentifiers(scopeTree)
    val newStatement = rewriteStatement(namespacer, statement)
    val newSemanticTable = rewriteSemanticTable(namespacer.identifierNames, semanticTable)

    (newStatement, newSemanticTable)
  }

  def rewriteStatement(namespacer: namespaceIdentifiers, statement: Statement): Statement = {
    val rewriter = RewriterStepSequencer.newDefault("Planner")(
      ApplyRewriter("namespaceIdentifiers", namespacer),

      rewriteEqualityToInCollection,
      splitInCollectionsToIsolateConstants,
      CNFNormalizer,
      collapseInCollectionsContainingConstants,
      nameUpdatingClauses,
      projectNamedPaths,
      projectFreshSortExpressions,
      inlineProjections
    )

    statement.endoRewrite(rewriter)
  }

  def rewriteSemanticTable(identifierNames: IdentifierNames, semanticTable: SemanticTable): SemanticTable = {
    val replacements = identifierNames.toSeq.collect {
      case ((oldName, oldPos), newName) => Identifier(oldName)(oldPos) -> Identifier(newName)(oldPos)
    }

    val newSemanticTable = semanticTable.replaceKeys(replacements: _*)
    newSemanticTable
  }
}

trait PlanningMonitor {
  def startedPlanning(q: String)
  def foundPlan(q: String, p: LogicalPlan)
  def successfulPlanning(q: String, p: PipeInfo)
}
