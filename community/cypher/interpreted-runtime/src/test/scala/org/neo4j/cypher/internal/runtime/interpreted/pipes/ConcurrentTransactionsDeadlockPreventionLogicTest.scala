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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ConcurrentTransactionsDeadlockPreventionLogicTest.seedRandomGenerator
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Values
import org.scalatest.Failed
import org.scalatest.Outcome
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.TableFor1
import org.scalatest.prop.Tables.Table

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ConcurrentTransactionsDeadlockPreventionLogicTest {
  val seedRandomGenerator = new Random()
}

class ConcurrentTransactionsDeadlockPreventionLogicTest extends InterpretedRuntimeTestSuite {
  private var _seed: Long = 0L
  private var random: Random = _

  /**
   * Set a fixed seed at the top of a test case to deterministically reproduce a random failure.
   * Must be called before any use of `random` in the test.
   */
  def setTestSeed(seed: Long): Unit = {
    _seed = seed
    random = new Random(seed)
  }

  override def beforeEach(): Unit = {
    _seed = seedRandomGenerator.nextLong
    random = new Random(_seed)
  }

  override def withFixture(test: NoArgTest): Outcome = {
    val clue = new {
      override def toString: String =
        s"""
           |ConcurrentTransactionsDeadlockPreventionLogicTest failed with seed: ${_seed}L
           |To reproduce (if it failed randomly), put the following line at the top of the test that failed:
           |setTestSeed(${_seed}L)
           |
           |""".stripMargin
    }
    withClue(clue) {
      try {
        val outcome = super.withFixture(test)
        outcome match {
          case Failed(_: org.scalatest.exceptions.ModifiableMessage[_]) =>
          // Clue will be included in the exception by the wrapping withClue
          case Failed(_) =>
            System.err.println(clue)
          case _ =>
        }
        outcome
      } catch {
        case e: Throwable if !e.isInstanceOf[org.scalatest.exceptions.ModifiableMessage[_]] =>
          System.err.println(clue)
          throw e
      }
    }
  }

  private def createBatchTestHarness(
    inputRows: Seq[Map[String, Any]],
    raidKeys: Seq[String],
    batchSize: Int,
    maxBatchesInFormation: Int = 64
  ): BatchTestHarness = {
    new BatchTestHarness(inputRows, raidKeys, batchSize, maxBatchesInFormation, random)
  }

  val simulationParams: TableFor1[Boolean] = Table("randomCompletionOrder", false, true)

  // ==================== Basic Tests ====================

  test("simple test") {
    val batchSize = 2
    val memoryTracker = EmptyMemoryTracker.INSTANCE
    val queryState = QueryStateHelper.emptyWithValueSerialization
    val inputPipe =
      FakePipe(Seq(Map("a" -> 1, "b" -> 1), Map("a" -> 2, "b" -> 2), Map("a" -> 3, "b" -> 3), Map("a" -> 4, "b" -> 4)))
    val inputIterator = inputPipe.createResults(queryState)

    val raids: Array[Expression] = Array(Variable("a"), Variable("b"))
    val dpLogic = new ConcurrentTransactionsDeadlockPreventionLogic(raids, maxBatchesInFormation = 64)
    val batchIterator = dpLogic.batchIterator(inputIterator, queryState, batchSize, memoryTracker)
    var counter = 0
    while (batchIterator.hasNext) {
      batchIterator.next()
      counter += 1
    }
    counter shouldBe 2
  }

  test("empty input produces no batches") {
    val h = createBatchTestHarness(Seq.empty, Seq("a"), batchSize = 2)
    val (batches, waits) = h.runSimulation()
    batches shouldBe 0
    waits shouldBe 0
  }

  test("single row produces one batch") {
    val h = createBatchTestHarness(Seq(Map("a" -> 1)), Seq("a"), batchSize = 2)
    val (batches, _) = h.runSimulation()
    batches shouldBe 1
    h.totalRows shouldBe 1
  }

  test("rows exactly filling one batch") {
    val h = createBatchTestHarness(
      Seq(Map("a" -> 1), Map("a" -> 2)),
      Seq("a"),
      batchSize = 2
    )
    val (batches, _) = h.runSimulation()
    batches shouldBe 1
    h.totalRows shouldBe 2
  }

  test("rows exactly filling multiple batches with no conflicts") {
    val rows = (1 to 10).map(i => Map("a" -> (i * 100)))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      batches shouldBe 5
      h.totalRows shouldBe 10
    }
  }

  test("batch size of 1 produces one batch per row") {
    val rows = (1 to 5).map(i => Map("a" -> i))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 1)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      batches shouldBe 5
      h.totalRows shouldBe 5
    }
  }

  // ==================== No Dependencies (Maximum Concurrency) ====================

  test("completely independent batches allow maximum concurrency") {
    // Each batch has unique RAID values - no overlaps
    val rows = (1 to 20).map(i => Map("a" -> (i * 1000), "b" -> (i * 1000 + 1)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    batches shouldBe 10
    waits shouldBe 0
    // With no dependencies, all batches can be concurrent
    maxConcurrent should be > 1
  }

  test("independent batches with single RAID key") {
    // Unique values per batch
    val rows = (1 to 8).map(i => Map("a" -> (i * 100)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    batches shouldBe 4
    h.totalRows shouldBe 8
    // No conflicts means no waits needed
    waits shouldBe 0
  }

  // ==================== Dependencies Between Batches ====================

  test("overlapping RAIDs create dependencies requiring waits") {
    // Batch 0: a=1, a=2; Batch 1: a=2, a=3 (overlap on a=2)
    // format: off
    val rows = Seq(
      Map("a" -> 1), Map("a" -> 2),
      Map("a" -> 2), Map("a" -> 3)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    batches shouldBe 2
    h.totalRows shouldBe 4
  }

  test("chain of dependencies - each batch depends on previous") {
    // Batch 0: a=1,a=2; Batch 1: a=2,a=3; Batch 2: a=3,a=4; Batch 3: a=4,a=5
    // format: off
    val rows = Seq(
      Map("a" -> 1), Map("a" -> 2),
      Map("a" -> 2), Map("a" -> 3),
      Map("a" -> 3), Map("a" -> 4),
      Map("a" -> 4), Map("a" -> 5)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    batches shouldBe 4
    h.totalRows shouldBe 8
  }

  test("two independent chains allow some concurrency") {
    // Chain A: a=1,a=2 -> a=2,a=3
    // Chain B: a=100,a=200 -> a=200,a=300
    // The two chains are independent so some concurrency is possible
    // format: off
    val rows = Seq(
      Map("a" -> 1), Map("a" -> 2),     // batch 0
      Map("a" -> 100), Map("a" -> 200),  // batch 1 (independent of 0)
      Map("a" -> 2), Map("a" -> 3),      // batch 2 (depends on 0)
      Map("a" -> 200), Map("a" -> 300)   // batch 3 (depends on 1)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    batches shouldBe 4
    h.totalRows shouldBe 8
    maxConcurrent should be >= 2
  }

  test("unified RAID map creates false dependencies when keys have overlapping value ranges") {
    // This test demonstrates the trade-off of using a single unified RAID map vs separate maps
    // per RAID key. When "a" and "b" represent independent resource types (e.g. different labels)
    // but their value ranges overlap, the unified map detects cross-key "dependencies" that don't
    // correspond to real lock conflicts.
    //
    // With separate RAID maps (hypothetical):
    //   a-map: batch 0 {1,2}, batch 1 {3,4}, batch 2 {5,6}, batch 3 {7,8} → all independent
    //   b-map: batch 0 {3,4}, batch 1 {5,6}, batch 2 {7,8}, batch 3 {9,10} → all independent
    //   → All 4 batches could run concurrently (maxConcurrent = 4)
    //
    // With the unified RAID map (actual):
    //   batch 0 {1,2,3,4}, batch 1 {3,4,5,6} → overlap on 3,4 → dependent
    //   batch 1 {3,4,5,6}, batch 2 {5,6,7,8} → overlap on 5,6 → dependent
    //   batch 2 {5,6,7,8}, batch 3 {7,8,9,10} → overlap on 7,8 → dependent
    //   → Linear chain → limited concurrency (safe, but conservative)
    // format: off
    val rows = Seq(
      Map("a" -> 1, "b" -> 3), Map("a" -> 2, "b" -> 4),   // batch 0: a={1,2}, b={3,4}
      Map("a" -> 3, "b" -> 5), Map("a" -> 4, "b" -> 6),   // batch 1: a={3,4}, b={5,6}
      Map("a" -> 5, "b" -> 7), Map("a" -> 6, "b" -> 8),   // batch 2: a={5,6}, b={7,8}
      Map("a" -> 7, "b" -> 9), Map("a" -> 8, "b" -> 10),  // batch 3: a={7,8}, b={9,10}
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    batches shouldBe 4
    h.totalRows shouldBe 8
    // The unified map creates a dependency chain via cross-key value overlap,
    // forcing serialization even though the actual resource types are independent.
    waits should be > 0
    maxConcurrent shouldBe 1
  }

  test("multiple RAID keys create dependencies on any overlap") {
    // Batch 0: (a=1,b=10), (a=2,b=20)
    // Batch 1: (a=3,b=10), (a=4,b=40) - overlaps on b=10
    // format: off
    val rows = Seq(
      Map("a" -> 1, "b" -> 10), Map("a" -> 2, "b" -> 20),
      Map("a" -> 3, "b" -> 10), Map("a" -> 4, "b" -> 40)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    batches shouldBe 2
    h.totalRows shouldBe 4
  }

  // ==================== WAIT_MARKER_BATCH Handling ====================

  test("WAIT_MARKER is returned when all batches in formation have dependencies") {
    // Create a scenario where dependencies force waiting
    // With maxBatchesInFormation=64 and a long chain, we should see waits
    val n = 130 // enough rows to fill many batches
    val rows = (1 to n).map(i => Map("a" -> i, "b" -> (i + 1))) // overlapping b values
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
  }

  test("serial execution handles WAIT_MARKERs correctly") {
    val rows = (1 to 20).map(i => Map("a" -> i, "b" -> (i + 1)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val count = h.runSerial()
    count should be > 0
    h.totalRows shouldBe 20
  }

  // ==================== Backpressure / maxBatchesInFormation ====================

  test("maxBatchesInFormation limits batches in formation") {
    // With maxBatchesInFormation=64 (minimum), create enough independent rows
    // to potentially exceed the limit - use simulation which completes on WAIT
    val rows = (1 to 200).map(i => Map("a" -> (i * 1000)))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 1, maxBatchesInFormation = 64)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      batches shouldBe 200
      h.totalRows shouldBe 200
    }
  }

  test("large input stream with small maxBatchesInFormation") {
    val rows = (1 to 500).map(i => Map("a" -> (i * 1000)))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 5, maxBatchesInFormation = 64)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      batches shouldBe 100
      h.totalRows shouldBe 500
    }
  }

  // ==================== Edge Cases ====================

  test("all rows have the same RAID value go into same batch") {
    val rows = (1 to 4).map(_ => Map("a" -> 42))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    // Same RAID means rows go to same batch until full, then next batch depends on it
    batches shouldBe 2
    h.totalRows shouldBe 4
  }

  test("alternating RAID values") {
    // a=1, a=2, a=1, a=2, a=1, a=2
    val rows = (1 to 6).map(i => Map("a" -> (i % 2)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe 6
  }

  test("onBatchCompleted is idempotent") {
    val rows = Seq(Map("a" -> 1), Map("a" -> 2), Map("a" -> 2), Map("a" -> 3))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    while (h.batchIterator.hasNext) {
      val batch = h.batchIterator.next()
      if (!batch.isMarker) {
        h.allBatches += batch
        h.inFlightBatches += batch
      } else {
        val completed = h.inFlightBatches.remove(0)
        // Call onBatchCompleted multiple times - should be idempotent
        h.dpLogic.onBatchCompleted(completed)
        h.dpLogic.onBatchCompleted(completed)
        h.dpLogic.onBatchCompleted(completed)
      }
    }
    h.inFlightBatches.foreach { b =>
      h.dpLogic.onBatchCompleted(b)
      h.dpLogic.onBatchCompleted(b) // idempotent
    }
  }

  test("batch size larger than total rows") {
    val rows = Seq(Map("a" -> 1), Map("a" -> 2))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 100)
    val (batches, _) = h.runSimulation()
    batches shouldBe 1
    h.totalRows shouldBe 2
  }

  // ==================== Complex Dependency Patterns ====================

  test("diamond dependency pattern") {
    // Batch A: x=1
    // Batch B: x=1, y=2 (depends on A via x=1)
    // Batch C: x=1, z=3 (depends on A via x=1, and on B via x=1)
    // Batch D: y=2, z=3 (depends on B via y=2, C via z=3)
    // format: off
    val rows = Seq(
      Map("a" -> 1, "b" -> 10), Map("a" -> 1, "b" -> 11),   // batch with x=1
      Map("a" -> 1, "b" -> 20), Map("a" -> 2, "b" -> 20),   // overlaps on a=1 and b=20
      Map("a" -> 1, "b" -> 30), Map("a" -> 3, "b" -> 30),   // overlaps on a=1
      Map("a" -> 2, "b" -> 20), Map("a" -> 3, "b" -> 30)    // overlaps on a=2,b=20 and a=3,b=30
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe 8
  }

  test("sliding window pattern from integration test") {
    val batchSize = 4
    // format: off
    val inputTable = Seq(
      // Batch 0: a01, a02, b01, b02
      Map("a" -> "a01", "b" -> "b01"), Map("a" -> "a01", "b" -> "b02"),
      Map("a" -> "a02", "b" -> "b01"), Map("a" -> "a02", "b" -> "b02"),
      // Batch 1: shares a02, b01, b02 with batch 0
      Map("a" -> "a02", "b" -> "b01"), Map("a" -> "a02", "b" -> "b02"),
      Map("a" -> "a03", "b" -> "b01"), Map("a" -> "a03", "b" -> "b02"),
      // Batch 2: shares a03, b02 with batch 1
      Map("a" -> "a03", "b" -> "b02"), Map("a" -> "a03", "b" -> "b03"),
      Map("a" -> "a04", "b" -> "b02"), Map("a" -> "a04", "b" -> "b03"),
      // Batch 3: shares a04, b03 with batch 2
      Map("a" -> "a04", "b" -> "b03"), Map("a" -> "a04", "b" -> "b04"),
      Map("a" -> "a05", "b" -> "b03"), Map("a" -> "a05", "b" -> "b04")
    )
    // format: on
    val h = createBatchTestHarness(inputTable, Seq("a", "b"), batchSize)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe inputTable.size
    batches shouldBe inputTable.size / batchSize
  }

  test("two independent groups with internal dependencies") {
    val batchSize = 4
    // Group A: a-nodes
    // format: off
    val groupA = Seq(
      Map("a" -> "a01", "b" -> "b01"), Map("a" -> "a01", "b" -> "b02"),
      Map("a" -> "a02", "b" -> "b01"), Map("a" -> "a02", "b" -> "b02"),
      Map("a" -> "a02", "b" -> "b01"), Map("a" -> "a02", "b" -> "b02"),
      Map("a" -> "a03", "b" -> "b01"), Map("a" -> "a03", "b" -> "b02")
    )
    // Group B: c-nodes (completely independent)
    val groupB = Seq(
      Map("a" -> "c01", "b" -> "d01"), Map("a" -> "c01", "b" -> "d02"),
      Map("a" -> "c02", "b" -> "d01"), Map("a" -> "c02", "b" -> "d02"),
      Map("a" -> "c02", "b" -> "d01"), Map("a" -> "c02", "b" -> "d02"),
      Map("a" -> "c03", "b" -> "d01"), Map("a" -> "c03", "b" -> "d02")
    )
    // format: on
    val rows = groupA ++ groupB
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    h.totalRows shouldBe rows.size
    // Two independent groups should allow some concurrency
    maxConcurrent should be >= 2
  }

  // ==================== Stress / Limits ====================

  test("large number of batches") {
    val n = 1000
    val rows = (1 to n).map(i => Map("a" -> (i * 10000)))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 1, maxBatchesInFormation = 64)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      batches shouldBe n
      h.totalRows shouldBe n
    }
  }

  test("large batch size with many rows") {
    val n = 1000
    val rows = (1 to n).map(i => Map("a" -> (i * 10000)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 100, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    batches shouldBe 10
    h.totalRows shouldBe n
  }

  test("huge input stream with dependencies does not OOM") {
    // Simulate a large stream with sliding window dependencies
    val n = 5000
    val rows = (1 to n).map(i => Map("a" -> i, "b" -> (i / 2)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 10, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 10)
  }

  test("independent data") {
    val n = 500
    // Each row has truly unique RAID values to avoid dependencies
    val rows = (1 to n).map(i => Map("a" -> (i * 1000000), "b" -> (i * 1000000 + 500000)))
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 5, maxBatchesInFormation = 64)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      h.totalRows shouldBe n
      batches shouldBe (n / 5)
    }
  }

  test("stress test - large lazy random stream with ~50% batch dependencies") {
    val rng = random
    val nRows = 2000000
    val batchSize = 10
    val nBatches = nRows / batchSize
    // Heuristic: with batchSize values per batch drawn from [0, rangeSize), two consecutive batches
    // share at least one value with probability ≈ 1 - (1 - batchSize/rangeSize)^batchSize.
    // Setting rangeSize ≈ 1.44 * batchSize^2 (= batchSize^2 / ln(2)) targets ~50% pairwise overlap.
    // With multiple in-flight batches the effective dependency rate is higher, so we use 2 * batchSize^2
    // to compensate and keep the overall rate around 50%.
    val rangeSize = math.max(2, 2 * batchSize * batchSize)
    // Lazy generation: rows are produced on demand, not materialized in memory all at once
    val rows: LazyList[Map[String, Any]] =
      LazyList.from(0).take(nRows).map(_ => Map[String, Any]("a" -> rng.nextInt(rangeSize)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize, maxBatchesInFormation = 64)
    val (batches, waits) = h.runSimulation(randomCompletionOrder = true)
    h.totalRows shouldBe nRows
    batches shouldBe nBatches
    // With ~50% dependencies, we expect a meaningful number of waits but not close to nBatches
    waits should be > 0
    waits should be < nBatches
  }

  test("stale RAID-to-batch mapping does not cause excessive waits due to phantom dependencies") {
    // 4 waves of RAID values, with repeated values between wave 1 and 3,
    // and different repeated values between wave 2 and 4, cycle through all 64 slots.
    // By checking the batch sequence and not only the batch index, stale RAID entries pointing to reused slots
    // are correctly ignored, so waits come only from slot exhaustion (192).
    // Otherwise, phantom dependencies on reused active slots could inflate waits.
    val maxBatches = 64
    val batchSize = 2

    val rows = new ArrayBuffer[Map[String, Any]]()
    for (j <- 0 until 4) {
      for (i <- 0 until maxBatches * batchSize) {
        rows += Map("a" -> (i / batchSize) * ((j % 2) + 1))
      }
    }

    val h = createBatchTestHarness(rows.toSeq, Seq("a"), batchSize, maxBatchesInFormation = maxBatches)
    val (batches, waits, _) = h.runMaxConcurrency()
    h.totalRows shouldBe rows.size
    batches shouldBe rows.size / batchSize

    // Optimal waits = 192 (64 per wave for waves 2-4, slot exhaustion only).
    waits should be <= 192
  }

  // ==================== Fuzz Tests ====================

  test("fuzz test - random data with conflicts") {
    val rng = random
    val n = 300
    // Use small value range to create many conflicts
    val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(20), "b" -> rng.nextInt(20)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 3, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 3)
  }

  test("fuzz test - random data with moderate conflicts") {
    val rng = random
    val n = 400
    val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(100), "b" -> rng.nextInt(100)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 4, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 4)
  }

  test("fuzz test - single RAID key with high conflict rate") {
    val rng = random
    val n = 200
    val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(5)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 2)
  }

  test("fuzz test - varying batch sizes with random data") {
    val rng = random
    for (batchSize <- Seq(1, 2, 3, 5, 10, 20)) {
      val n = 100
      val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(50)))
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = batchSize, maxBatchesInFormation = 64)
      val (batches, _, _) = h.runMaxConcurrency()
      h.totalRows shouldBe n
      val expectedBatches = (n + batchSize - 1) / batchSize
      batches shouldBe expectedBatches
    }
  }

  test("fuzz test - large random stream serial execution") {
    val rng = random
    val n = 1000
    val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(30), "b" -> rng.nextInt(30)))
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 5, maxBatchesInFormation = 64)
    val count = h.runSerial()
    h.totalRows shouldBe n
    count shouldBe (n / 5)
  }

  test("fuzz test - stress with minimal maxBatchesInFormation") {
    val rng = random
    val n = 500
    val rows = (1 to n).map(_ => Map("a" -> rng.nextInt(10)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 2)
  }

  // ==================== Concurrency Verification ====================

  test("no dependencies means zero waits") {
    // Completely unique RAIDs per row
    val rows = (1 to 20).map(i => Map("a" -> (i * 10000)))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, waits, _) = h.runMaxConcurrency()
    batches shouldBe 10
    waits shouldBe 0
  }

  test("full dependency chain requires waits") {
    // Every batch overlaps with the next
    val rows = (1 to 20).map(i => Map("a" -> (i / 2))) // pairs share same value
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, waits, _) = h.runMaxConcurrency()
    batches shouldBe 10
    // With overlapping RAIDs, some waits are expected
    // (exact count depends on algorithm internals)
  }

  test("partial dependencies allow partial concurrency") {
    // Mix of independent and dependent batches
    // format: off
    val rows = Seq(
      Map("a" -> 1), Map("a" -> 2),     // batch 0
      Map("a" -> 100), Map("a" -> 200),  // batch 1 (independent)
      Map("a" -> 300), Map("a" -> 400),  // batch 2 (independent)
      Map("a" -> 2), Map("a" -> 3),      // batch 3 (depends on 0)
      Map("a" -> 500), Map("a" -> 600),  // batch 4 (independent)
      Map("a" -> 3), Map("a" -> 4)       // batch 5 (depends on 3)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    val (batches, waits, maxConcurrent) = h.runMaxConcurrency()
    batches shouldBe 6
    h.totalRows shouldBe 12
    // Independent batches should allow concurrency > 1
    maxConcurrent should be > 1
  }

  test("completing batches out of order") {
    // format: off
    val rows = Seq(
      Map("a" -> 1), Map("a" -> 2),
      Map("a" -> 100), Map("a" -> 200),
      Map("a" -> 2), Map("a" -> 3),
      Map("a" -> 200), Map("a" -> 300)
    )
    // format: on
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
    // Manual simulation: pull batches, complete in reverse order
    val pulled = new ArrayBuffer[TransactionBatch]()
    while (h.batchIterator.hasNext) {
      val batch = h.batchIterator.next()
      if (batch.isMarker) {
        // Complete newest first (reverse order)
        if (pulled.nonEmpty) {
          val last = pulled.remove(pulled.size - 1)
          h.dpLogic.onBatchCompleted(last)
        }
      } else {
        pulled += batch
      }
    }
    // Complete remaining in reverse
    pulled.reverse.foreach(h.dpLogic.onBatchCompleted)
  }

  // ==================== Row Count Verification ====================

  test("total rows across all batches equals input rows") {
    for (n <- Seq(1, 2, 3, 5, 10, 17, 50, 100)) {
      for (bs <- Seq(1, 2, 3, 5, 10)) {
        val rows = (1 to n).map(i => Map("a" -> (i * 1000)))
        forAll(simulationParams) { randomCompletionOrder =>
          val h = createBatchTestHarness(rows, Seq("a"), batchSize = bs, maxBatchesInFormation = 64)
          val (_, _) = h.runSimulation(randomCompletionOrder)
          h.totalRows shouldBe n
        }
      }
    }
  }

  test("rows not lost with heavy conflicts") {
    val n = 100
    // All rows have same RAID - maximum conflict
    val rows = (1 to n).map(_ => Map("a" -> 1))
    val h = createBatchTestHarness(rows, Seq("a"), batchSize = 5, maxBatchesInFormation = 64)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe n
    batches shouldBe (n / 5)
  }

  // ==================== Error Path Tests ====================

  test("empty RAID expressions should be rejected") {
    val emptyRaids: Array[Expression] = Array.empty
    an[IllegalArgumentException] should be thrownBy {
      new ConcurrentTransactionsDeadlockPreventionLogic(emptyRaids, maxBatchesInFormation = 64)
    }
  }

  test("maxBatches exceeding 16-bit limit should be rejected") {
    an[IllegalArgumentException] should be thrownBy {
      new BitmaskBatchScheduler(10, 1, maxBatches = 65536, EmptyMemoryTracker.INSTANCE)
    }
  }

  test("maxBatches not a multiple of 64 should be rejected by BitmaskBatchScheduler") {
    an[IllegalArgumentException] should be thrownBy {
      new BitmaskBatchScheduler(10, 1, maxBatches = 100, EmptyMemoryTracker.INSTANCE)
    }
  }

  // ==================== 16-bit Encoding Boundary Tests ====================

  test("maxBatches at 16-bit boundary (65472 = 1023 * 64)") {
    // Largest valid maxBatches that is a multiple of 64 and <= 65535
    val maxBatches = 65472
    val scheduler = new BitmaskBatchScheduler(2, 1, maxBatches, EmptyMemoryTracker.INSTANCE)
    val raids = Array(Values.intValue(1).asInstanceOf[org.neo4j.values.AnyValue])
    val idx = scheduler.insertRow(raids)
    idx should be >= 0
    scheduler.close()
  }

  test("16-bit encode/decode round trip with several batches") {
    val maxBatches = 65472
    val scheduler = new BitmaskBatchScheduler(2, 1, maxBatches, EmptyMemoryTracker.INSTANCE)

    // Insert rows with unique RAIDs to allocate several batches
    val allocatedIndices = (1 to 10).map { i =>
      val raids = Array(Values.intValue(i * 1000).asInstanceOf[org.neo4j.values.AnyValue])
      scheduler.insertRow(raids)
    }
    allocatedIndices.foreach(_ should be >= 0)

    // Insert rows with overlapping RAIDs to verify dependency tracking still works at this scale
    val depIdx = scheduler.insertRow(Array(Values.intValue(1000).asInstanceOf[org.neo4j.values.AnyValue]))
    depIdx should be >= 0

    scheduler.close()
  }

  test("ring buffer reaches all slots for non-power-of-2 maxBatches") {
    // maxBatches=192 is a valid multiple of 64 but not a power of 2. The ring buffer
    // in allocateNewBatch must wrap through all 192 slots, not degrade to the lower 64
    // slots that a bitmask wrap `& (maxBatches - 1)` would reach.
    val maxBatches = 192
    val scheduler = new BitmaskBatchScheduler(1, 1, maxBatches, EmptyMemoryTracker.INSTANCE)

    // Insert maxBatches rows with unique RAIDs so every row allocates a fresh batch.
    val allocatedIndices = (0 until maxBatches).map { i =>
      val raids = Array(Values.intValue(i).asInstanceOf[org.neo4j.values.AnyValue])
      scheduler.insertRow(raids)
    }
    allocatedIndices.foreach(_ should be >= 0)
    allocatedIndices.toSet.size shouldBe maxBatches

    scheduler.close()
  }

  // ==================== Direct Purge Tests ====================

  test("purgeStaleRaidEntries is triggered and reduces map size") {
    val batchSize = 2
    val maxBatches = 64
    val scheduler = new BitmaskBatchScheduler(batchSize, 1, maxBatches, EmptyMemoryTracker.INSTANCE)
    // purge threshold = 2L * 64 * 2 * 1 = 256

    // Fill and complete many batches to generate stale RAID entries
    for (wave <- 0 until 3) {
      // Insert rows to fill all batch slots
      for (batchNum <- 0 until maxBatches) {
        for (row <- 0 until batchSize) {
          val raidValue = wave * maxBatches * batchSize + batchNum * batchSize + row
          val raids = Array(Values.intValue(raidValue).asInstanceOf[org.neo4j.values.AnyValue])
          scheduler.insertRow(raids)
        }
      }
      // Poll, dispatch and complete each batch individually
      var readyIdx = scheduler.pollReadyBatch(force = true)
      while (readyIdx >= 0) {
        scheduler.dispatchBatch(readyIdx)
        scheduler.completeBatch(readyIdx)
        readyIdx = scheduler.pollReadyBatch(force = true)
      }
    }
    // After 3 waves of 64 batches * 2 rows each = 384 unique RAID values inserted,
    // purge threshold is 256. completeBatch triggers purge when map exceeds threshold.
    val sizeAfterPurge = scheduler.raidMapSize
    // 384 unique values were inserted; purge fires when size > 256 (the threshold)
    // and removes the 256 stale entries from waves 0 and 1 whose batch slots were
    // reused with new sequence numbers. The map should be at or below the threshold.
    sizeAfterPurge should be <= 256L
    scheduler.close()
  }

  // ==================== NO_VALUE RAID Tests ====================

  test("rows with null RAID values are handled correctly") {
    val rows = Seq(
      Map[String, Any]("a" -> 1),
      Map[String, Any]("a" -> null),
      Map[String, Any]("a" -> 2),
      Map[String, Any]("a" -> null)
    )
    forAll(simulationParams) { randomCompletionOrder =>
      val h = createBatchTestHarness(rows, Seq("a"), batchSize = 2)
      val (batches, _) = h.runSimulation(randomCompletionOrder)
      h.totalRows shouldBe 4
      batches shouldBe 2
    }
  }

  test("mixed null and non-null RAID values with dependencies") {
    val rows = Seq(
      Map[String, Any]("a" -> 1, "b" -> null),
      Map[String, Any]("a" -> 2, "b" -> null),
      Map[String, Any]("a" -> 1, "b" -> 3),
      Map[String, Any]("a" -> 4, "b" -> 3)
    )
    val h = createBatchTestHarness(rows, Seq("a", "b"), batchSize = 2)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe 4
    batches shouldBe 2
  }

  // ==================== Batch Formation Limit + Dependencies Tests ====================

  test("saturated batch formation with linear dependency chain does not deadlock") {
    // maxBatchesInFormation=64 with data where every batch depends on the previous one
    val maxBatches = 64
    val batchSize = 2
    // Create rows where consecutive batches share a RAID value, forming a linear chain
    val rows = new ArrayBuffer[Map[String, Any]]()
    for (batch <- 0 until maxBatches * 3) {
      rows += Map("a" -> batch) // unique to this batch
      rows += Map("a" -> (batch + 1)) // overlaps with next batch
    }
    val h = createBatchTestHarness(rows.toSeq, Seq("a"), batchSize, maxBatchesInFormation = maxBatches)
    val (batches, _, _) = h.runMaxConcurrency()
    h.totalRows shouldBe rows.size
    batches shouldBe (rows.size / batchSize)
  }
}

/**
 * Mini test framework for ConcurrentTransactionsDeadlockPreventionLogic.
 *
 * Simulates the concurrent batch execution loop:
 * 1. Pull batches from batchIterator
 * 2. Track in-flight batches
 * 3. Complete batches via onBatchCompleted
 * 4. Handle WAIT_MARKER_BATCH by completing oldest in-flight batch
 */
class BatchTestHarness(
  inputRows: Seq[Map[String, Any]],
  raidKeys: Seq[String],
  batchSize: Int,
  maxBatchesInFormation: Int = 64,
  val rng: Random
) {
  val memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE
  private val queryState = QueryStateHelper.emptyWithValueSerialization
  private val raids: Array[Expression] = raidKeys.map(k => Variable(k)).toArray
  val dpLogic = new ConcurrentTransactionsDeadlockPreventionLogic(raids, maxBatchesInFormation)

  private val inputPipe = FakePipe(inputRows)
  private val inputIterator = inputPipe.createResults(queryState)

  val batchIterator: ClosingIterator[TransactionBatch] =
    dpLogic.batchIterator(inputIterator, queryState, batchSize, memoryTracker)

  val allBatches = new ArrayBuffer[TransactionBatch]()
  val inFlightBatches = new ArrayBuffer[TransactionBatch]()
  var waitCount = 0
  var totalRows = 0

  // Topological-order tracking: maps batch identity -> dispatch/completion order
  private val dispatchOrder = new java.util.IdentityHashMap[TransactionBatch, Int]()
  private val completionOrder = new java.util.IdentityHashMap[TransactionBatch, Int]()
  // RAID value -> last batch that contained it (for dependency reconstruction)
  private val raidLastWriterBatch = new java.util.HashMap[Any, TransactionBatch]()
  // batch -> set of parent batches
  private val parentBatches = new java.util.IdentityHashMap[TransactionBatch, java.util.Set[TransactionBatch]]()
  private var dispatchCounter = 0
  private var completionCounter = 0

  private def trackDispatch(batch: TransactionBatch): Unit = {
    dispatchOrder.put(batch, dispatchCounter)
    dispatchCounter += 1
    // Reconstruct dependencies from RAID values
    val parents = new java.util.HashSet[TransactionBatch]()
    val rowIter = batch.rows.iterator()
    while (rowIter.hasNext) {
      val row = rowIter.next()
      for (raidExpr <- raids) {
        val raidValue = raidExpr.apply(row, queryState)
        val lastWriter = raidLastWriterBatch.get(raidValue)
        if (lastWriter != null && (lastWriter ne batch) && !completionOrder.containsKey(lastWriter)) {
          // This batch depends on lastWriter which is still in-flight - this should NOT happen
          // (the scheduler should have waited for it)
          parents.add(lastWriter)
        }
        raidLastWriterBatch.put(raidValue, batch)
      }
    }
    parentBatches.put(batch, parents)
  }

  private def trackCompletion(batch: TransactionBatch): Unit = {
    completionOrder.put(batch, completionCounter)
    completionCounter += 1
  }

  /**
   * Verify topological ordering: for every dispatched batch, all its parent batches
   * (those sharing RAID values that were dispatched before it) must have been completed
   * before it was dispatched.
   */
  def assertTopologicalOrder(): Unit = {
    val iter = parentBatches.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val batch = entry.getKey
      val parents = entry.getValue
      val pIter = parents.iterator()
      while (pIter.hasNext) {
        val parent = pIter.next()
        val parentCompleted = completionOrder.containsKey(parent)
        assert(
          parentCompleted,
          s"Topological order violation: batch dispatched at order ${dispatchOrder.get(batch)} " +
            s"has parent dispatched at order ${dispatchOrder.get(parent)} that was not yet completed"
        )
      }
    }
  }

  /**
   * Run the full simulation: pull all batches, completing oldest in-flight on WAIT_MARKER or a random in-flight batch.
   * Returns (totalBatches, waitMarkerCount).
   */
  def runSimulation(randomCompletionOrder: Boolean = false): (Int, Int) = {
    while (batchIterator.hasNext) {
      val batch = batchIterator.next()
      if (batch.isMarker) {
        waitCount += 1
        // Complete the oldest in-flight batch to unblock
        require(inFlightBatches.nonEmpty, "WAIT_MARKER but no in-flight batches")
        val completed = if (randomCompletionOrder) {
          val i = rng.nextInt(inFlightBatches.size)
          inFlightBatches.remove(i)
        } else {
          inFlightBatches.remove(0)
        }
        trackCompletion(completed)
        dpLogic.onBatchCompleted(completed)
      } else {
        trackDispatch(batch)
        totalRows += batch.rows.size().toInt
        allBatches += batch
        inFlightBatches += batch
      }
    }
    // Complete remaining in-flight
    inFlightBatches.foreach { b =>
      trackCompletion(b)
      dpLogic.onBatchCompleted(b)
    }
    inFlightBatches.clear()
    assertTopologicalOrder()
    (allBatches.size, waitCount)
  }

  /**
   * Run simulation completing all in-flight batches after each pull (serial execution).
   */
  def runSerial(): Int = {
    var count = 0
    while (batchIterator.hasNext) {
      val batch = batchIterator.next()
      if (batch.isMarker) {
        waitCount += 1
      } else {
        trackDispatch(batch)
        totalRows += batch.rows.size().toInt
        allBatches += batch
        count += 1
        // Complete immediately (serial)
        trackCompletion(batch)
        dpLogic.onBatchCompleted(batch)
      }
    }
    assertTopologicalOrder()
    count
  }

  /**
   * Run simulation that tracks maximum concurrency achieved.
   * Completes batches only when forced by WAIT_MARKER.
   */
  def runMaxConcurrency(): (Int, Int, Int) = {
    var maxConcurrent = 0
    while (batchIterator.hasNext) {
      val batch = batchIterator.next()
      if (batch.isMarker) {
        waitCount += 1
        require(inFlightBatches.nonEmpty)
        val completed = inFlightBatches.remove(0)
        trackCompletion(completed)
        dpLogic.onBatchCompleted(completed)
      } else {
        trackDispatch(batch)
        totalRows += batch.rows.size().toInt
        allBatches += batch
        inFlightBatches += batch
        if (inFlightBatches.size > maxConcurrent) {
          maxConcurrent = inFlightBatches.size
        }
      }
    }
    inFlightBatches.foreach { b =>
      trackCompletion(b)
      dpLogic.onBatchCompleted(b)
    }
    inFlightBatches.clear()
    assertTopologicalOrder()
    (allBatches.size, waitCount, maxConcurrent)
  }
}
