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

import org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.ScalaTestDeflaker
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionTerminationTestBase.TEST_TRANSACTION_TIMEOUT
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.TransactionTerminatedException
import org.neo4j.internal.helpers.ArrayUtil

import java.time.Duration

import scala.collection.mutable.ArrayBuffer

// NOTE: Be careful when you are retuning these tests to avoid flakiness.
abstract class TransactionTerminationTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should terminate long running pruning var-length-expand") {
    // given
    givenGraph {
      connectedNestedStarGraph(5, 7, "START", "R") // 19608 nodes
    }

    // when
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..11]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    runAndAssertTransactionTimeout(logicalQuery)
  }

  test("should terminate long running bfs pruning var-length-expand") {
    // given
    givenGraph {
      connectedNestedStarGraph(5, 7, "START", "R") // 19608 nodes
    }

    // when
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("42 AS result")
      .prober(sleepProbe(100))
      .bfsPruningVarExpand("(x)-[*0..11]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    runAndAssertTransactionTimeout(logicalQuery)
  }

  test("should terminate long running var-length-expand") {
    // given
    givenGraph {
      connectedNestedStarGraph(5, 7, "START", "R") // 19608 nodes
    }

    // when
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("42 AS result")
      .prober(sleepProbe(100))
      .expand("(x)-[*0..]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    runAndAssertTransactionTimeout(logicalQuery)
  }

  test("should terminate long running stateful shortest path") {
    // given
    givenGraph {
      val tx = runtimeTestSupport.tx

      // relatively small graph with high connectivity - means there exponentially many paths (2^(size-2)) of different lengths
      // which must all be traced/propagated after a single expansion
      val size = 25 // 8388608 paths

      val nodes = new ArrayBuffer[Node]()

      for (_ <- 0 until size) {
        val n = tx.createNode()
        for (m <- nodes) {
          m.createRelationshipTo(n, RelationshipType.withName("R"))
        }
        nodes += n
      }

      nodes.head.addLabel(Label.label("START"))
      nodes.last.addLabel(Label.label("END"))
    }

    // when
    val nfa = new TestNFABuilder(0, "s")
      .addTransition(0, 1, "(s) (a_nfa)")
      .addTransition(1, 2, "(a_nfa)-[r_nfa:R]->(b_nfa)")
      .addTransition(2, 3, "(b_nfa) (e_nfa:END)")
      .addTransition(2, 1, "(b_nfa) (a_nfa)")
      .setFinalState(3)
      .build()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("e")
      .statefulShortestPath(
        "s",
        "e",
        "",
        Some("size(r) >= 25"), // make sure the algorithm is tied up and not yielding rows
        Set("a_nfa" -> "a", "b_nfa" -> "b"),
        Set("r_nfa" -> "r"),
        Set("e_nfa" -> "e"),
        Set.empty,
        Selector.ShortestGroups(Int.MaxValue),
        nfa,
        ExpandAll,
        false
      )
      .nodeByLabelScan("s", "START", IndexOrderNone)
      .build()

    runAndAssertTransactionTimeout(logicalQuery)
  }

  test("should terminate long running unwind") {
    // given an empty db

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .prober(sleepProbe(10))
      .unwind(s"range(1, ${ArrayUtil.MAX_ARRAY_SIZE}) AS i")
      .argument()
      .build()

    runAndAssertTransactionTimeout(logicalQuery)
  }

  private def sleepProbe(sleepFor: Long) = new Prober.Probe {

    override def onRow(row: AnyRef, state: AnyRef): Unit = {
      Thread.sleep(sleepFor)
    }
  }

  private def runAndAssertTransactionTimeout(logicalQuery: LogicalQuery): Unit = {
    updateDynamicSetting(transaction_timeout, TEST_TRANSACTION_TIMEOUT)
    restartTx()
    val testDeflaker = ScalaTestDeflaker(
      acceptInstantSuccess = true,
      retries = 3,
      toleratedFailures = 1,
      sleepMs = 1000,
      printToleratedFailuresTo = Some(System.out)
    )
    try {
      testDeflaker.apply(
        () => {
          val runtimeResult = execute(logicalQuery, runtime)

          val startTimeConsume = System.currentTimeMillis()
          a[TransactionTerminatedException] should be thrownBy {
            consume(runtimeResult)
            println("Finished after:" + (System.currentTimeMillis() - startTimeConsume))
          }
          val stopTime = System.currentTimeMillis()
          val responseTimeConsumeMs = stopTime - startTimeConsume

          // Add a hefty margin to the expected response time to reduce flakiness
          val maxExpectedResponseTimeLimitMs = (TEST_TRANSACTION_TIMEOUT.toMillis * 3.0).toLong
          responseTimeConsumeMs should be < maxExpectedResponseTimeLimitMs

          // Make sure the test is doing sufficient work in the consume phase (approximately)
          val minExpectedResponseTimeLimitMs = (TEST_TRANSACTION_TIMEOUT.toMillis * 0.6).toLong
          responseTimeConsumeMs should be >= minExpectedResponseTimeLimitMs
        },
        afterEachAttempt = () => {
          rollbackAndRestartTx()
        }
      )
    } finally {
      updateDynamicSetting(transaction_timeout, transaction_timeout.defaultValue())
      restartTx()
    }
  }
}

object TransactionTerminationTestBase {
  val TEST_TRANSACTION_TIMEOUT: Duration = Duration.ofSeconds(8L)
}
