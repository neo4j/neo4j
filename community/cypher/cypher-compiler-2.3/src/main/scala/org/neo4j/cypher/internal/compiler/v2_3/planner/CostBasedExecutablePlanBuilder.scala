/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.ast.conditions.containsNamedPathOnlyForShortestPath
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutablePlanBuilder, NewRuntimeSuccessRateMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.closing
import org.neo4j.cypher.internal.compiler.v2_3.planner.execution.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.{ApplyRewriter, RewriterCondition, RewriterStepSequencer}

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class CostBasedExecutablePlanBuilder(monitors: Monitors,
                                          metricsFactory: MetricsFactory,
                                          tokenResolver: SimpleTokenResolver,
                                          queryPlanner: QueryPlanner,
                                          queryGraphSolver: QueryGraphSolver,
                                          rewriterSequencer: (String) => RewriterStepSequencer,
                                          plannerName: CostBasedPlannerName,
                                          runtimeBuilder: RuntimeBuilder)
  extends ExecutablePlanBuilder {

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext, tracer: CompilationPhaseTracer) = {
    val statement =
      CostBasedExecutablePlanBuilder.rewriteStatement(
        statement = inputQuery.statement,
        scopeTree = inputQuery.scopeTree,
        semanticTable = inputQuery.semanticTable,
        rewriterSequencer = rewriterSequencer,
        preConditions = inputQuery.conditions,
        monitor = monitors.newMonitor[AstRewritingMonitor]()
      )

    //monitor success of compilation
    val planBuilderMonitor = monitors.newMonitor[NewRuntimeSuccessRateMonitor](CypherCompilerFactory.monitorTag)
    statement match {
      case (ast: Query, rewrittenSemanticTable) =>
        val (logicalPlan, pipeBuildContext) = closing(tracer.beginPhase(LOGICAL_PLANNING)) {
          produceLogicalPlan(ast, rewrittenSemanticTable)(planContext)
        }
        runtimeBuilder(logicalPlan, pipeBuildContext, planContext, tracer, rewrittenSemanticTable, planBuilderMonitor,
                      plannerName, inputQuery)
      case _ =>
        throw new CantHandleQueryException
    }
  }

  def produceLogicalPlan(ast: Query, semanticTable: SemanticTable)
                        (planContext: PlanContext): (LogicalPlan, PipeExecutionBuilderContext) = {
    tokenResolver.resolve(ast)(semanticTable, planContext)
    val unionQuery = ast.asUnionQuery

    val metrics = metricsFactory.newMetrics(planContext.statistics)
    val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
    val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, semanticTable, queryGraphSolver)
    val plan = queryPlanner.plan(unionQuery)(context)

    val pipeBuildContext = PipeExecutionBuilderContext(metrics.cardinality, semanticTable, plannerName)

    (plan, pipeBuildContext)
  }
}

object CostBasedExecutablePlanBuilder {
  import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStep._

  def rewriteStatement(statement: Statement, scopeTree: Scope, semanticTable: SemanticTable, rewriterSequencer: (String) => RewriterStepSequencer, preConditions: Set[RewriterCondition], monitor: AstRewritingMonitor): (Statement, SemanticTable) = {
    val namespacer = Namespacer(statement, scopeTree)
    val newStatement = rewriteStatement(namespacer, statement, rewriterSequencer, preConditions, monitor)
    val newSemanticTable = namespacer.tableRewriter(semanticTable)
    (newStatement, newSemanticTable)
  }

  private def rewriteStatement(namespacer: Namespacer, statement: Statement, rewriterSequencer: (String) => RewriterStepSequencer, preConditions: Set[RewriterCondition], monitor: AstRewritingMonitor): Statement = {
    val rewriter =
      rewriterSequencer("Planner")
        .withPrecondition(preConditions)(
          ApplyRewriter("namespaceIdentifiers", namespacer.astRewriter),
          rewriteEqualityToInCollection,
          CNFNormalizer()(monitor),
          collapseInCollections,
          nameUpdatingClauses /* this is actually needed as a precondition for projectedNamedPaths even though we do not handle updates in Ronja */,
          projectNamedPaths,
          enableCondition(containsNamedPathOnlyForShortestPath),
          projectFreshSortExpressions
        ).rewriter

    statement.endoRewrite(rewriter)
  }
}

