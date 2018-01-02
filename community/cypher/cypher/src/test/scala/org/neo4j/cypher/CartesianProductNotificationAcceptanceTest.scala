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
package org.neo4j.cypher

import org.mockito.Matchers._
import org.mockito.Mockito.{verify, _}
import org.neo4j.cypher.internal.compatibility.{EntityAccessorWrapper2_3, StringInfoLogger2_3, WrappedMonitors2_3}
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.frontend.v2_3.notification.CartesianProductNotification
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.logging.NullLog

class CartesianProductNotificationAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {

  test("should warn when disconnected patterns") {
    //given
    val logger = mock[InternalNotificationLogger]
    val compiler = createCompiler()

    //when
    graph.inTx {
      compiler.planQuery("MATCH (a)-->(b), (c)-->(d) RETURN *", planContext, logger)
    }

    //then
    verify(logger, times(1)) += CartesianProductNotification(InputPosition(0, 1, 1), Set("c", "d"))
  }

  test("should not warn when connected patterns") {
    //given
    val logger = mock[InternalNotificationLogger]
    val compiler = createCompiler()

    //when
    graph.inTx(compiler.planQuery("MATCH (a)-->(b), (a)-->(c) RETURN *", planContext, logger))

    //then
    verify(logger, never) += any()
  }

  test("should warn when one disconnected pattern in otherwise connected pattern") {
    //given
    val logger = mock[InternalNotificationLogger]
    val compiler = createCompiler()

    //when
    graph.inTx {
      compiler.planQuery("MATCH (a)-->(b), (b)-->(c), (x)-->(y), (c)-->(d), (d)-->(e) RETURN *", planContext, logger)
    }

    //then
    verify(logger, times(1)) += CartesianProductNotification(InputPosition(0, 1, 1), Set("x", "y"))
  }

  test("should not warn when disconnected patterns in multiple match clauses") {
    //given
    val logger = mock[InternalNotificationLogger]
    val compiler = createCompiler()

    //when
    graph.inTx(compiler.planQuery("MATCH (a)-->(b) MATCH (c)-->(d) RETURN *", planContext, logger))

    //then
    verify(logger, never) += any()
  }

  test("this query does not contain a cartesian product") {
    //given
    val logger = mock[InternalNotificationLogger]
    val compiler = createCompiler()

    //when
    graph.inTx(compiler.planQuery("""MATCH (p)-[r1]-(m),
                                    |(m)-[r2]-(d), (d)-[r3]-(m2)
                                    |RETURN DISTINCT d""".stripMargin, planContext, logger))

    //then
    verify(logger, never) += any()
  }

  private def createCompiler() = {
    val nodeManager = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[NodeManager])

    CypherCompilerFactory.costBasedCompiler(
      graph,
      new EntityAccessorWrapper2_3(nodeManager),
      CypherCompilerConfiguration(
        queryCacheSize = 128,
        statsDivergenceThreshold = 0.5,
        queryPlanTTL = 1000L,
        useErrorsOverWarnings = false,
        idpMaxTableSize = 128,
        idpIterationDuration = 1000,
        nonIndexedLabelWarningThreshold = 10000L
      ),
      Clock.SYSTEM_CLOCK,
      new WrappedMonitors2_3(kernelMonitors),
      new StringInfoLogger2_3(NullLog.getInstance),
      plannerName = Some(GreedyPlannerName),
      runtimeName = Some(InterpretedRuntimeName),
      rewriterSequencer = RewriterStepSequencer.newValidating
    )
  }
}
