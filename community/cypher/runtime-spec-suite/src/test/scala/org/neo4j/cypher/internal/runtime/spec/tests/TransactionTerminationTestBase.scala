package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.ScalaTestDeflaker
import org.neo4j.cypher.internal.runtime.spec.tests.TransactionTerminationTestBase.TRANSACTION_TIMEOUT_SECONDS
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.TransactionTerminatedException
import org.neo4j.graphdb.config.Setting

import java.time.Duration

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.SECONDS

// NOTE: Be careful when you are retuning these tests to avoid flakiness.
//       Creating the graph also need to complete within the timeout
abstract class TransactionTerminationTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        GraphDatabaseSettings.transaction_timeout -> Duration.ofSeconds(TRANSACTION_TIMEOUT_SECONDS)
      ),
      runtime
    ) {

  test("should terminate long running pruning var-length-expand") {
    // given
    val (_, nNodes) = givenGraph {
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

  private def runAndAssertTransactionTimeout(logicalQuery: LogicalQuery): Unit = {
    val testDeflaker = ScalaTestDeflaker(
      acceptInstantSuccess = true,
      retries = 3,
      toleratedFailures = 1,
      sleepMs = 1000,
      printToleratedFailuresTo = Some(System.out)
    )
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
        val maxExpectedResponseTimeLimitMs = SECONDS.toMillis((TRANSACTION_TIMEOUT_SECONDS * 1.5).toLong)
        responseTimeConsumeMs should be < maxExpectedResponseTimeLimitMs

        // Make sure the test is doing sufficient work in the consume phase (approximately)
        val minExpectedResponseTimeLimitMs = SECONDS.toMillis((TRANSACTION_TIMEOUT_SECONDS * 0.6).toLong)
        responseTimeConsumeMs should be >= minExpectedResponseTimeLimitMs
      },
      afterEachAttempt = () => {
        rollbackAndRestartTx()
      }
    )
  }

}

object TransactionTerminationTestBase {
  val TRANSACTION_TIMEOUT_SECONDS: Long = 8L
}
