/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.time.Clock

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{verify, _}
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.compatibility.v3_5.WrappedMonitors
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.{PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.planner.v3_5.spi.{IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, TransactionBoundPlanContext, TransactionalContextWrapper}
import org.neo4j.kernel.api.{KernelTransaction, Statement}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.opencypher.v9_0.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, devNullLogger}
import org.opencypher.v9_0.rewriting.RewriterStepSequencer
import org.opencypher.v9_0.util.attribution.SequentialIdGen
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{CartesianProductNotification, InputPosition}

class CartesianProductNotificationAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {
  var logger: InternalNotificationLogger = _
  var compiler: CypherPlanner[PlannerContext] = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    logger = mock[InternalNotificationLogger]
    compiler = createCompiler()
  }

  test("should warn when disconnected patterns") {
    //when
    runQuery("MATCH (a)-->(b), (c)-->(d) RETURN *")

    //then
    verify(logger, times(1)).log(CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d")))
  }

  test("should not warn when connected patterns") {
    //when
    runQuery("MATCH (a)-->(b), (a)-->(c) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("should warn when one disconnected pattern in otherwise connected pattern") {
    //when
    runQuery("MATCH (a)-->(b), (b)-->(c), (x)-->(y), (c)-->(d), (d)-->(e) RETURN *")

    //then
    verify(logger, times(1)).log(CartesianProductNotification(InputPosition(0, 1, 1), Set("x", "y")))
  }

  test("should not warn when disconnected patterns in multiple match clauses") {
    //when
    runQuery("MATCH (a)-->(b) MATCH (c)-->(d) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("this query does not contain a cartesian product") {
    //when
    runQuery(
      """MATCH (p)-[r1]-(m),
        |(m)-[r2]-(d), (d)-[r3]-(m2)
        |RETURN DISTINCT d""".stripMargin)

    //then
    verify(logger, never).log(any())
  }

  private def runQuery(query: String) = {
    graph.inTx {
      val tracer = CompilationPhaseTracer.NO_TRACING
      val parsed = compiler.parseQuery(query, query, logger, IDPPlannerName.name, Set.empty, None, tracer)
      val kernelTransaction = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).getKernelTransactionBoundToThisThread(true)
      val statement = kernelTransaction.acquireStatement()
      val context = PlannerContextCreator.create(tracer, logger, planContext(kernelTransaction, statement), parsed.queryText, Set.empty,
        None, monitors, metricsFactory, createQueryGraphSolver(), configuration, defaultUpdateStrategy, Clock.systemUTC(), new SequentialIdGen(),
                                                         simpleExpressionEvaluator)

      try {
        val normalized = compiler.normalizeQuery(parsed, context)
        compiler.planPreparedQuery(normalized, context)
      }
      finally {
        statement.close()
      }
    }
  }
  private val configuration = CypherPlannerConfiguration(
    queryCacheSize = 128,
    statsDivergenceCalculator = StatsDivergenceCalculator.divergenceNoDecayCalculator(0.5, 1000),
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    csvBufferSize = CSVResources.DEFAULT_BUFFER_SIZE,
    nonIndexedLabelWarningThreshold = 10000L,
    planWithMinimumCardinalityEstimates = false
  )
  private lazy val monitors = WrappedMonitors(kernelMonitors)
  private val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
  private def createCompiler(): CypherPlanner[PlannerContext] = {

    new CypherPlannerFactory().costBasedCompiler(
      configuration,
      Clock.systemUTC(),
      monitors,
      rewriterSequencer = RewriterStepSequencer.newValidating,
      updateStrategy = None,
      contextCreator = PlannerContextCreator
    )
  }

  private def planContext(transaction: KernelTransaction, statement: Statement): PlanContext = {
    val tc = mock[TransactionalContextWrapper]
    when(tc.dataRead).thenReturn(transaction.dataRead())
    when(tc.graph).thenReturn(graph)
    TransactionBoundPlanContext(tc, devNullLogger)
  }

  private def createQueryGraphSolver(): IDPQueryGraphSolver = {
    val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
    val solverConfig = new ConfigurableIDPSolverConfig(
      maxTableSize = configuration.idpMaxTableSize,
      iterationDurationLimit = configuration.idpIterationDuration
    )
    val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
    IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
  }
}
