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

import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.SchedulerSimulator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.greedy
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable
import scala.util.Random

/**
 * Focused coverage for the greedy [[BitmaskBatchScheduler]] at maxBatches > 64, where batch indices
 * span more than one 64-bit dependency word (ROW_WORDS > 1). The other scheduler suites all run at
 * maxBatches=64 (ROW_WORDS=1), so the cross-word bitmask paths in the shared dependency-DAG
 * maintenance are otherwise untested.
 */
class BitmaskBatchSchedulerTest extends CypherFunSuite {

  private def v(s: String): AnyValue = Values.stringValue(s)

  test("cross-word dependency: completing a word-0 parent releases its word-1 child (maxBatches=128)") {
    // maxBatches=128 gives ROW_WORDS=2, so batch slots split across two 64-bit words: word 0 holds
    // indices 0..63, word 1 holds indices 64..127. batchSize=1 makes every row seal its own batch,
    // so batches are allocated at strictly increasing ring-buffer slots 0,1,2,..., which lets us
    // place a parent in word 0 and its dependent child in word 1 deterministically.
    val named = greedy(bs = 1, rpr = 1, mb = 128)
    try {
      val sched = named.scheduler

      // Batch 0 (word 0) owns the shared RAID value.
      sched.insertRow(Array(v("shared"))) shouldBe 0
      // Fill out the rest of word 0 with independent batches 1..63 (distinct RAID values, no edges).
      var i = 1
      while (i < 64) {
        sched.insertRow(Array(v("k" + i))) shouldBe i
        i += 1
      }
      // Batch 64 lands in word 1 and shares the RAID value with batch 0. This is the load-bearing
      // step: it records a forwardDeps/reverseDeps edge that crosses the 64-bit word boundary
      // (reverseDeps(0)(1) and forwardDeps(64)(0)).
      sched.insertRow(Array(v("shared"))) shouldBe 64

      // Drain every filled+ready batch. Batches 0..63 are dependency-free and full, so they are all
      // pollable; batch 64 must stay blocked while its parent (batch 0) is only dispatched, not yet
      // completed.
      val dispatched = mutable.ArrayBuffer.empty[Int]
      var d = sched.pollReadyBatch(force = false)
      while (d >= 0) {
        sched.dispatchBatch(d)
        dispatched += d
        d = sched.pollReadyBatch(force = false)
      }
      dispatched.toSet shouldBe (0 until 64).toSet
      // Even a forced poll finds nothing: batch 64 is genuinely blocked on the still-in-flight
      // batch 0, not merely un-full.
      sched.pollReadyBatch(force = true) shouldBe -1

      // Completing the word-0 parent must notify its word-1 child across the word boundary and make
      // it ready+dispatchable. This is the exact path the cross-word test guards.
      sched.completeBatch(0)
      val released = sched.pollReadyBatch(force = false)
      released shouldBe 64

      // The released child groups the second "shared" row and is now safe to run (batch 0 completed).
      sched.dispatchBatch(released)
      sched.completeBatch(released)
    } finally named.close()
  }

  test("fixed scheduler heap is tracked on construction and released on close") {
    val maxBatches = 128
    val schedulers: Seq[(String, MemoryTracker => BatchScheduler)] = Seq(
      ("greedy", tracker => new BitmaskBatchScheduler(4, 2, maxBatches, tracker)),
      ("affinity", tracker => new AffinityBitmaskBatchScheduler(4, 2, maxBatches, tracker))
    )
    for ((name, mk) <- schedulers) {
      val tracker = new LocalMemoryTracker()
      val sched = mk(tracker)
      // The two dependency matrices alone hold maxBatches * ROW_WORDS longs each
      val matricesLowerBound = 2L * maxBatches * (maxBatches / 64) * java.lang.Long.BYTES
      assert(
        tracker.estimatedHeapMemory() >= matricesLowerBound,
        s"$name: tracked ${tracker.estimatedHeapMemory()} bytes, expected at least $matricesLowerBound"
      )
      sched.insertRow(Array(v("a"), v("b"))) shouldBe 0
      sched.close()
      assert(
        tracker.estimatedHeapMemory() == 0,
        s"$name: scheduler heap not fully released: ${tracker.estimatedHeapMemory()} bytes"
      )
    }
  }

  test("row conservation and raid-disjoint safety with cross-word batches (maxBatches=128)") {
    // A recurring-value workload that keeps far more than 64 batches live at once, so the ring
    // buffer allocates well into word 1 and dependency edges routinely straddle the word boundary.
    // The simulator asserts the safety invariant (in-flight batches are raid-disjoint) on every
    // dispatch and that no rows are lost, exercising the shared cross-word DAG maintenance broadly.
    for {
      seed <- Seq(1L, 42L, 777L)
      batchSize <- Seq(2, 10)
    } {
      val rng = new Random(seed)
      val rows: Array[Array[AnyValue]] =
        Array.fill(4000)(Array[AnyValue](v("v" + rng.nextInt(300))))
      val named = greedy(batchSize, 1, 128)
      try {
        val sim = new SchedulerSimulator(named, batchSize, capacity = 8)
        rows.foreach(sim.insert)
        sim.drain()
        assert(
          sim.rowsIn == rows.length && sim.rowsOut == rows.length,
          s"lost rows in=${sim.rowsIn} out=${sim.rowsOut} (seed=$seed bs=$batchSize)"
        )
      } finally named.close()
    }
  }
}
