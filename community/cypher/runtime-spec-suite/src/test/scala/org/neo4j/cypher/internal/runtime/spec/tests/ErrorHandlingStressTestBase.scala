/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.runtime.spec.SideEffectingInputStream
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.LogAssert
import org.scalatest.LoneElement

import java.time.Duration.ofSeconds

import scala.util.Random

abstract class ErrorHandlingStressTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseInternalSettings.cypher_query_monitor_check_interval -> ofSeconds(3)
      ),
      runtime
    )
    with SideEffectingInputStream[CONTEXT]
    with RandomValuesTestSupport
    with LoneElement {

  override protected def createRuntimeTestSupport(
    graphDb: GraphDatabaseService,
    edition: Edition[CONTEXT],
    runtime: CypherRuntime[CONTEXT],
    workloadMode: Boolean,
    logProvider: InternalLogProvider
  ): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](
      graphDb,
      edition,
      runtime,
      workloadMode,
      logProvider,
      debugOptions,
      defaultTransactionType = Type.IMPLICIT
    )
  }

  class SuperFatalError(msg: String) extends VirtualMachineError(msg)

  test("should log accumulated crash reports") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val nQueries = 300
    val errorMessage = "Simulated fatal error of type "
    val nRandomMessages = 10
    val orderedMessages = (1 to nRandomMessages).map(errorMessage + _)
    val errorMessages = Random.shuffle(orderedMessages)
    var errorCount = 0

    val fatalProbe = new Probe {
      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        errorCount = (errorCount + 1) % nRandomMessages
        throw new SuperFatalError(errorMessages(errorCount))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .prober(fatalProbe)
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)
    (0 until nQueries).foreach { _ =>
      val result = execute(executablePlan)
      a[SuperFatalError] shouldBe thrownBy(consume(result))
    }

    val logAssert = new LogAssert(logProvider)
    logAssert.containsMessagesEventually(
      10000,
      (s"Cypher query monitor received $nRandomMessages different crash reports" +: orderedMessages): _*
    )
    // logProvider.print(System.out)
  }
}
