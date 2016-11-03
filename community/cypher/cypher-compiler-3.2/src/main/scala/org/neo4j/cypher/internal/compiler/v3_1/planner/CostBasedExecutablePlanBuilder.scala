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
package org.neo4j.cypher.internal.compiler.v3_1.planner

import org.neo4j.cypher.internal.compiler.v3_1.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.ast.conditions.containsNamedPathOnlyForShortestPath
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutablePlanBuilder, NewRuntimeSuccessRateMonitor, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_1.helpers.closing
import org.neo4j.cypher.internal.compiler.v3_1.planner.execution.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.{ApplyRewriter, RewriterCondition, RewriterStep, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.{InternalException, Scope, SemanticTable}

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class CostBasedExecutablePlanBuilder(monitors: Monitors,
                                          metricsFactory: MetricsFactory,
                                          tokenResolver: SimpleTokenResolver,
                                          queryPlanner: QueryPlanner,
                                          queryGraphSolver: QueryGraphSolver,
                                          rewriterSequencer: (String) => RewriterStepSequencer,
                                          semanticChecker: SemanticChecker,
                                          plannerName: CostBasedPlannerName,
                                          runtimeBuilder: RuntimeBuilder,
                                          updateStrategy: UpdateStrategy,
                                          config: CypherCompilerConfiguration,
                                          publicTypeConverter: Any => Any)
  extends ExecutablePlanBuilder {

  override def producePlan(inputQuery: PreparedQuerySemantics, planContext: PlanContext, tracer: CompilationPhaseTracer,
                           createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference) = {
    val statement =
      CostBasedExecutablePlanBuilder.rewriteStatement(
        statement = inputQuery.statement,
        scopeTree = inputQuery.scopeTree,
        semanticTable = inputQuery.semanticTable,
        rewriterSequencer = rewriterSequencer,
        preConditions = inputQuery.conditions,
        monitor = monitors.newMonitor[AstRewritingMonitor](),
        semanticChecker = semanticChecker)

    //monitor success of compilation
    val planBuilderMonitor = monitors.newMonitor[NewRuntimeSuccessRateMonitor](CypherCompilerFactory.monitorTag)

    statement match {
      case (ast: Query, rewrittenSemanticTable) =>
        val (periodicCommit, logicalPlan, pipeBuildContext) = closing(tracer.beginPhase(LOGICAL_PLANNING)) {
          produceLogicalPlan(ast, rewrittenSemanticTable)(planContext, planContext.notificationLogger())
        }
          runtimeBuilder(periodicCommit, logicalPlan, pipeBuildContext, planContext, tracer, rewrittenSemanticTable,
                         planBuilderMonitor, plannerName, inputQuery, createFingerprintReference, config)
      case x =>
        throw new CantHandleQueryException(x.toString())
    }
  }

  def produceLogicalPlan(ast: Query, semanticTable: SemanticTable)
                        (planContext: PlanContext,
                         notificationLogger: InternalNotificationLogger):
  (Option[PeriodicCommit], LogicalPlan, PipeExecutionBuilderContext) = {

    tokenResolver.resolve(ast)(semanticTable, planContext)
    val unionQuery = toUnionQuery(ast, semanticTable)
    val metrics = metricsFactory.newMetrics(planContext.statistics)
    val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)

    val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, semanticTable,
      queryGraphSolver, notificationLogger = notificationLogger, useErrorsOverWarnings = config.useErrorsOverWarnings,
      errorIfShortestPathFallbackUsedAtRuntime = config.errorIfShortestPathFallbackUsedAtRuntime,
      config = QueryPlannerConfiguration.default.withUpdateStrategy(updateStrategy))

    val (periodicCommit, plan) = queryPlanner.plan(unionQuery)(context)

    val pipeBuildContext = PipeExecutionBuilderContext(metrics.cardinality, semanticTable, plannerName)

    //Check for unresolved tokens for read-only queries
    if (plan.solved.all(_.queryGraph.readOnly)) checkForUnresolvedTokens(ast, semanticTable).foreach(notificationLogger += _)

    (periodicCommit, plan, pipeBuildContext)
  }
}

object CostBasedExecutablePlanBuilder {
  import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStep._

  def rewriteStatement(statement: Statement,
                       scopeTree: Scope,
                       semanticTable: SemanticTable,
                       rewriterSequencer: (String) => RewriterStepSequencer,
                       semanticChecker: SemanticChecker,
                       preConditions: Set[RewriterCondition],
                       monitor: AstRewritingMonitor): (Statement, SemanticTable) = {
    val statementRewriter = StatementRewriter(rewriterSequencer, preConditions, monitor)
    val namespacer = Namespacer(statement, scopeTree)
    val namespacedStatement = statementRewriter.rewriteStatement(statement)(
      ApplyRewriter("Namespacer", namespacer.statementRewriter),
      rewriteEqualityToInPredicate,
      CNFNormalizer()(monitor)
    )

    val state = semanticChecker.check(namespacedStatement.toString, namespacedStatement, mkException = (msg, pos) => throw new InternalException(s"Unexpected error during late semantic checking: $msg at $pos"))
    val table = semanticTable.copy(types = state.typeTable, recordedScopes = state.recordedScopes)

    val newStatement = statementRewriter.rewriteStatement(namespacedStatement)(
      collapseMultipleInPredicates,
      nameUpdatingClauses /* this is actually needed as a precondition for projectedNamedPaths even though we do not handle updates in Ronja */ ,
      projectNamedPaths,
      enableCondition(containsNamedPathOnlyForShortestPath),
      projectFreshSortExpressions
    )
    (newStatement, table)
  }

  case class StatementRewriter(rewriterSequencer: (String) => RewriterStepSequencer, preConditions: Set[RewriterCondition], monitor: AstRewritingMonitor) {
    def rewriteStatement(statement: Statement)(steps: RewriterStep*): Statement = {
      val rewriter = rewriterSequencer("Planner").withPrecondition(preConditions)(steps: _*).rewriter
      statement.endoRewrite(rewriter)
    }
  }
}

