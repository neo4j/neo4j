/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.GraphIcing
import org.neo4j.cypher.internal.compatibility.v3_4.WrappedMonitors
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp.{IDPQueryGraphSolver, IDPQueryGraphSolverMonitor, SingleComponentPlanner, cartesianProductsOrValueJoins}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{ASTRewriter, Never}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_4.spi.{IDPPlannerName, PlanContext, PlannerNameFor}
import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundPlanContext, TransactionBoundQueryContext, TransactionalContextWrapper, ValueConversion}
import org.neo4j.cypher.internal.runtime.{InternalExecutionResult, NormalMode}
import org.neo4j.cypher.internal.util.v3_4.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.{CompilerEngineDelegator, ExecutionPlan, RewindableExecutionResult}
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo.EMBEDDED_CONNECTION
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, TransactionalContextFactory}
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.util.Try

object CypherReductionSupport {
  private val rewriterSequencer = RewriterStepSequencer.newValidating _
  private val astRewriter = new ASTRewriter(rewriterSequencer, literalExtraction = Never, getDegreeRewriting = true)
  private val stepSequencer = RewriterStepSequencer.newPlain _
  private val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
  private val config = CypherCompilerConfiguration(
    queryCacheSize = 0,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0, 0),
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = false,
    legacyCsvQuoteEscaping = false,
    nonIndexedLabelWarningThreshold = 0)
  private val kernelMonitors = new Monitors
  private val compiler = CypherCompiler(astRewriter, WrappedMonitors(kernelMonitors), stepSequencer, metricsFactory, config, defaultUpdateStrategy,
    CompilerEngineDelegator.CLOCK, CommunityRuntimeContextCreator)

  private val monitor = kernelMonitors.newMonitor(classOf[IDPQueryGraphSolverMonitor])
  private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])
  private val singleComponentPlanner = SingleComponentPlanner(monitor)
  private val queryGraphSolver = IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

  val prettifier = Prettifier(ExpressionStringifier())
}

/**
  * Do not mixin GraphDatabaseTestSupport when using this object.
  */
trait CypherReductionSupport extends CypherTestSupport with GraphIcing {
  self: CypherFunSuite  =>

  private var graph: GraphDatabaseCypherService = _
  private var contextFactory: TransactionalContextFactory = _

  override protected def initTest() {
    super.initTest()
    graph = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
    contextFactory = Neo4jTransactionalContextFactory.create(graph, new PropertyContainerLocker())
  }

  override protected def stopTest() {
    try {
      super.stopTest()
    }
    finally {
      if (graph != null) graph.shutdown()
    }
  }



  private val rewriting = PreparatoryRewriting andThen
    SemanticAnalysis(warn = true).adds(BaseContains[SemanticState])
  private val createExecPlan = ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
    If((s: CompilationState) => s.maybeExecutionPlan.isFailure)(
      CommunityRuntimeBuilder.create(None, CypherReductionSupport.config.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan]))



  def evaluate(query: String): InternalExecutionResult = {
    val parsingBaseState = queryToParsingBaseState(query)
    val statement = parsingBaseState.statement()
    produceResult(query, statement, parsingBaseState, None)
  }

  def reduceQuery(query: String, executeBefore: Option[String] = None)(test: Oracle[Try[InternalExecutionResult]]): String = {
    val parsingBaseState = queryToParsingBaseState(query)
    val statement = parsingBaseState.statement()

    val oracle: Oracle[Statement] = (currentStatement) => {
      // Actual query
      val tryResults = Try(produceResult(query, currentStatement, parsingBaseState, executeBefore))
      val testRes = test(tryResults)
      testRes
    }

    val smallerStatement = GTRStar(new StatementGTRInput(statement))(oracle)
    CypherReductionSupport.prettifier.asString(smallerStatement)
  }

  private def queryToParsingBaseState(query: String): BaseState = {
    val startState = LogicalPlanState(query, None, PlannerNameFor(IDPPlannerName.name))
    val parsingContext = createContext(query, CypherReductionSupport.metricsFactory, CypherReductionSupport.config, null, null)
    CompilationPhases.parsing(CypherReductionSupport.stepSequencer).transform(startState, parsingContext)
  }

  private def produceResult(query: String,
                            statement: Statement,
                            parsingBaseState: BaseState,
                            executeBefore: Option[String]): InternalExecutionResult = {
    val explicitTx = graph.beginTransaction(Transaction.Type.explicit, SecurityContext.AUTH_DISABLED)
    val implicitTx = graph.beginTransaction(Transaction.Type.`implicit`, SecurityContext.AUTH_DISABLED)
    try {
      executeBefore match {
        case None =>
        case Some(setupQuery) =>
          val setupBS = queryToParsingBaseState(setupQuery)
          val setupStm = setupBS.statement()
          executeInTx(setupQuery, setupStm, setupBS, implicitTx)
      }
      executeInTx(query, statement, parsingBaseState, implicitTx)
    } finally {
      explicitTx.failure()
      explicitTx.close()
    }
  }

  private def executeInTx(query: String, statement: Statement, parsingBaseState: BaseState, implicitTx: InternalTransaction): InternalExecutionResult = {
    val neo4jtxContext = contextFactory.newContext(EMBEDDED_CONNECTION, implicitTx, query, EMPTY_MAP)
    val txContextWrapper = TransactionalContextWrapper(neo4jtxContext)
    val planContext = TransactionBoundPlanContext(txContextWrapper, devNullLogger)

    var baseState = parsingBaseState.withStatement(statement)
    val planningContext = createContext(query, CypherReductionSupport.metricsFactory, CypherReductionSupport.config, planContext, CypherReductionSupport.queryGraphSolver)


    baseState = rewriting.transform(baseState, planningContext)

    val logicalPlanState = CypherReductionSupport.compiler.planPreparedQuery(baseState, planningContext)


    val compilationState = createExecPlan.transform(logicalPlanState, planningContext)
    val executionPlan = compilationState.maybeExecutionPlan.get
    val queryContext = new TransactionBoundQueryContext(txContextWrapper)(CypherReductionSupport.searchMonitor)

    RewindableExecutionResult(executionPlan.run(queryContext, NormalMode, ValueConversion.asValues(baseState.extractedParams())))
  }

  private def createContext(query: String, metricsFactory: CachedMetricsFactory,
                            config: CypherCompilerConfiguration,
                            planContext: PlanContext,
                            queryGraphSolver: IDPQueryGraphSolver) = {
    // TODO enterpriseContext
    val logicalPlanIdGen = new SequentialIdGen()
    CommunityRuntimeContextCreator.create(NO_TRACING, devNullLogger, planContext, query, Set(),
      None, WrappedMonitors(new Monitors), metricsFactory, queryGraphSolver, config = config, updateStrategy = defaultUpdateStrategy, clock = CompilerEngineDelegator.CLOCK, logicalPlanIdGen, evaluator = null)
  }
}
