/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher

import java.time.Clock

import org.mockito.Matchers._
import org.mockito.Mockito.{verify, _}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CommunityRuntimeContext, CommunityRuntimeContextCreator}
import org.neo4j.cypher.internal.compatibility.v3_3.{Compatibility, WrappedMonitors}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.frontend.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.notification.CartesianProductNotification
import org.neo4j.cypher.internal.frontend.v3_4.phases.{CompilationPhaseTracer, InternalNotificationLogger}
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

class CartesianProductNotificationAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {
  var logger: InternalNotificationLogger = _
  var compiler: CypherCompiler[CommunityRuntimeContext] = _

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
    //given
    runQuery("MATCH (a)-->(b), (a)-->(c) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("should warn when one disconnected pattern in otherwise connected pattern") {
    //given
    runQuery("MATCH (a)-->(b), (b)-->(c), (x)-->(y), (c)-->(d), (d)-->(e) RETURN *")

    //then
    verify(logger, times(1)).log(CartesianProductNotification(InputPosition(0, 1, 1), Set("x", "y")))
  }

  test("should not warn when disconnected patterns in multiple match clauses") {
    //given
    runQuery("MATCH (a)-->(b) MATCH (c)-->(d) RETURN *")

    //then
    verify(logger, never).log(any())
  }

  test("this query does not contain a cartesian product") {
    //given
    val logger = mock[InternalNotificationLogger]

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
      val tracer =CompilationPhaseTracer.NO_TRACING
      val parsed = compiler.parseQuery(query, query, logger, IDPPlannerName.name, Set.empty, None, tracer)
      val queryGraphSolver = Compatibility.createQueryGraphSolver(IDPPlannerName, monitors, configuration)
      val statement = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
      val context = CommunityRuntimeContextCreator.create(tracer, logger, planContext(statement), parsed.queryText, Set.empty,
        None, monitors, metricsFactory, queryGraphSolver, configuration, defaultUpdateStrategy, Clock.systemUTC(),
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
  private val configuration = CypherCompilerConfiguration(
    queryCacheSize = 128,
    statsDivergenceThreshold = 0.5,
    queryPlanTTL = 1000L,
    useErrorsOverWarnings = false,
    idpMaxTableSize = 128,
    idpIterationDuration = 1000,
    errorIfShortestPathFallbackUsedAtRuntime = false,
    errorIfShortestPathHasCommonNodesAtRuntime = true,
    legacyCsvQuoteEscaping = false,
    nonIndexedLabelWarningThreshold = 10000L
  )
  private lazy val monitors = WrappedMonitors(kernelMonitors)
  private val metricsFactory = CachedMetricsFactory(SimpleMetricsFactory)
  private def createCompiler(): CypherCompiler[CommunityRuntimeContext] = {

    new CypherCompilerFactory().costBasedCompiler(
      configuration,
      Clock.systemUTC(),
      monitors,
      rewriterSequencer = RewriterStepSequencer.newValidating,
      plannerName = None,
      updateStrategy = None,
      contextCreator = CommunityRuntimeContextCreator
    )
  }
}
