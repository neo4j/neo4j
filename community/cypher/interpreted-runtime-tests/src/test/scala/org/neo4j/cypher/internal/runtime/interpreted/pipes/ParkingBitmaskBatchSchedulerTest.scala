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

import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.NamedScheduler
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.SchedulerSimulator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.affinity
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.greedy
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AffinityBitmaskBatchSchedulerTest.makespan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable
import scala.util.Random

object ParkingBitmaskBatchSchedulerTest {

  def parking(
    bs: Int,
    rpr: Int,
    mb: Int,
    budget: Int = 0,
    strikeLimit: Int = ParkingBitmaskBatchScheduler.DEFAULT_STRIKE_LIMIT,
    hotQueueCap: Int = 0
  ): NamedScheduler = {
    val effectiveBudget = ParkingBitmaskBatchScheduler.effectiveParkingBudget(budget, mb, bs)
    NamedScheduler(
      "parking",
      new ParkingBitmaskBatchScheduler(
        bs,
        rpr,
        mb,
        effectiveBudget,
        EmptyMemoryTracker.INSTANCE,
        strikeLimit,
        hotQueueCap
      )
    )
  }

  /**
   * Simulator that additionally tracks row identities, batch completion order and per-batch
   * membership, for the per-key ordering property. FIFO completion with bounded capacity, like
   * SchedulerSimulator.
   */
  final class OrderTrackingSimulator(named: NamedScheduler, capacity: Int) {
    private val sched = named.scheduler

    private val formationRowIds = mutable.Map.empty[Int, mutable.ArrayBuffer[Int]]
    private val parkedBySlot = mutable.Map.empty[Int, Int]
    private val rowRaids = mutable.Map.empty[Int, Array[AnyValue]]
    private val inFlight = mutable.Queue.empty[(Int, Seq[Int])]

    // Batch membership in completion order
    val completedBatches: mutable.ArrayBuffer[Seq[Int]] = mutable.ArrayBuffer.empty

    private val sink: ParkedRowSink = new ParkedRowSink {
      override def place(parkedSlot: Int, batchIdx: Int): Unit = {
        val rowId = parkedBySlot.remove(parkedSlot)
          .getOrElse(throw new AssertionError(s"sink placed unknown park slot $parkedSlot"))
        formationRowIds.getOrElseUpdate(batchIdx, mutable.ArrayBuffer.empty) += rowId
      }
    }

    def insert(rowId: Int, raids: Array[AnyValue]): Unit = {
      rowRaids(rowId) = raids
      var result = sched.insertRow(raids)
      while (result == BatchScheduler.BACKPRESSURE) {
        val d = sched.pollReadyBatch(force = false)
        if (d >= 0) {
          dispatch(d)
        } else if (inFlight.nonEmpty) {
          completeOldest()
        } else {
          if (sched.hasParkedRows) {
            sched.admitParked(force = true, sink)
          }
          val f = sched.pollReadyBatch(force = true)
          assert(f >= 0, "liveness violation")
          dispatch(f)
          completeOldest()
        }
        result = sched.insertRow(raids)
      }
      if (result <= BatchScheduler.PARKED_BASE) {
        parkedBySlot(BatchScheduler.parkedSlot(result)) = rowId
      } else {
        formationRowIds.getOrElseUpdate(result, mutable.ArrayBuffer.empty) += rowId
      }
      var d = sched.pollReadyBatch(force = false)
      while (d >= 0) {
        dispatch(d)
        d = sched.pollReadyBatch(force = false)
      }
    }

    def drain(): Unit = {
      while (formationRowIds.nonEmpty || inFlight.nonEmpty || parkedBySlot.nonEmpty) {
        val d = sched.pollReadyBatch(force = false)
        if (d >= 0) {
          dispatch(d)
        } else if (inFlight.nonEmpty) {
          completeOldest()
        } else {
          // Force admission may stop early on batch-slot exhaustion, so a force poll must follow
          // to dispatch a batch and free slots, exactly like the real iterator's drain path
          if (parkedBySlot.nonEmpty) {
            sched.admitParked(force = true, sink)
          }
          val f = sched.pollReadyBatch(force = true)
          if (f >= 0) {
            dispatch(f)
          } else {
            assert(
              formationRowIds.isEmpty && parkedBySlot.isEmpty,
              s"liveness violation in drain (${formationRowIds.size} forming, ${parkedBySlot.size} parked)"
            )
          }
        }
      }
    }

    /**
     * Assert the per-key ordering property: for every RAID value, rows carrying it execute in
     * input order across batches (their row-id ranges must be non-overlapping and increasing in
     * batch completion order).
     */
    def assertPerKeyOrder(): Unit = {
      val perRaidBatches = mutable.Map.empty[AnyValue, mutable.ArrayBuffer[Seq[Int]]]
      completedBatches.foreach { rowIds =>
        val byRaid = mutable.Map.empty[AnyValue, mutable.ArrayBuffer[Int]]
        rowIds.foreach { rowId =>
          rowRaids(rowId).foreach { raid =>
            byRaid.getOrElseUpdate(raid, mutable.ArrayBuffer.empty) += rowId
          }
        }
        byRaid.foreach { case (raid, ids) =>
          perRaidBatches.getOrElseUpdate(raid, mutable.ArrayBuffer.empty) += ids.toSeq
        }
      }
      perRaidBatches.foreach { case (raid, batchSequences) =>
        var previousMax = -1
        batchSequences.foreach { ids =>
          val minId = ids.min
          assert(
            minId > previousMax,
            s"per-key order violated for $raid: row $minId completed after row $previousMax"
          )
          previousMax = ids.max
        }
      }
    }

    private def dispatch(idx: Int): Unit = {
      val rowIds = formationRowIds.remove(idx).map(_.toSeq).getOrElse(Seq.empty)
      assert(rowIds.nonEmpty, s"dispatched batch $idx with no rows")
      sched.dispatchBatch(idx)
      if (inFlight.size >= capacity) completeOldest()
      inFlight.enqueue((idx, rowIds))
    }

    private def completeOldest(): Unit = {
      val (idx, rowIds) = inFlight.dequeue()
      completedBatches += rowIds
      sched.completeBatch(idx)
      if (sched.hasParkedRows) {
        sched.admitParked(force = false, sink)
      }
    }
  }

  /**
   * Uniform-balanced circulant multigraph in a shuffled emission order: m active values per side,
   * degree d, every (a, b) pair unique. The workload family from the
   * ConcurrentTransactionRelationshipImport micro benchmark.
   */
  def shuffledCirculant(m: Int, d: Int, seed: Long): Array[Array[AnyValue]] = {
    val rows = Array.ofDim[Array[AnyValue]](m * d)
    var p = 0
    var k = 0
    while (k < d) {
      var i = 0
      while (i < m) {
        rows(p) = Array[AnyValue](Values.stringValue("a" + i), Values.stringValue("b" + ((i + k) % m)))
        p += 1
        i += 1
      }
      k += 1
    }
    val rng = new Random(seed)
    var j = rows.length - 1
    while (j > 0) {
      val swap = rng.nextInt(j + 1)
      val tmp = rows(j)
      rows(j) = rows(swap)
      rows(swap) = tmp
      j -= 1
    }
    rows
  }
}

class ParkingBitmaskBatchSchedulerTest extends CypherFunSuite {
  import ParkingBitmaskBatchSchedulerTest._

  private def v(s: String): AnyValue = Values.stringValue(s)

  private def parkingScheduler(named: NamedScheduler): ParkingBitmaskBatchScheduler =
    named.scheduler.asInstanceOf[ParkingBitmaskBatchScheduler]

  test("safety: raid-disjoint in-flight invariant and row conservation across random workloads") {
    for {
      seed <- Seq(1L, 42L, 777L, 31337L, 987654321L)
      batchSize <- Seq(2, 10, 64)
      raidsPerRow <- Seq(1, 2)
      cardinality <- Seq(8, 10000)
      maxBatches <- Seq(64, 128)
    } {
      val rng = new Random(seed)
      val rows: Array[Array[AnyValue]] =
        Array.fill(1000)(Array.fill[AnyValue](raidsPerRow)(v("v" + rng.nextInt(cardinality))))
      for (
        mk <- Seq(
          () => greedy(batchSize, raidsPerRow, maxBatches),
          () => affinity(batchSize, raidsPerRow, maxBatches),
          () => parking(batchSize, raidsPerRow, maxBatches)
        )
      ) {
        val named = mk()
        try {
          val sim = new SchedulerSimulator(named, batchSize, capacity = 8)
          rows.foreach(sim.insert)
          sim.drain()
          assert(
            sim.rowsIn == rows.length && sim.rowsOut == rows.length,
            s"${named.name}: row conservation violated (in=${sim.rowsIn}, out=${sim.rowsOut}) " +
              s"for seed=$seed bs=$batchSize rpr=$raidsPerRow card=$cardinality mb=$maxBatches"
          )
          assert(sim.parkedCount == 0, s"${named.name}: rows left parked after drain")
        } finally {
          named.close()
        }
      }
    }
  }

  test("per-key ordering: same-RAID rows execute in input order across batches") {
    for {
      seed <- Seq(7L, 99L, 4711L)
      cardinality <- Seq(4, 32, 500)
      batchSize <- Seq(2, 16)
    } {
      val rng = new Random(seed)
      val named = parking(batchSize, 2, 64)
      try {
        val sim = new OrderTrackingSimulator(named, capacity = 8)
        var rowId = 0
        while (rowId < 2000) {
          sim.insert(rowId, Array(v("a" + rng.nextInt(cardinality)), v("b" + rng.nextInt(cardinality))))
          rowId += 1
        }
        sim.drain()
        sim.assertPerKeyOrder()
      } finally {
        named.close()
      }
    }
  }

  test("per-key ordering holds with an aggressive hot-RAID cap and strike limit") {
    val rng = new Random(1234L)
    // 30% of rows share one hot value, forcing hot drains and strike-outs to interleave with
    // regular admission
    val named = parking(4, 2, 64, strikeLimit = 1, hotQueueCap = 4)
    try {
      val sim = new OrderTrackingSimulator(named, capacity = 4)
      var rowId = 0
      while (rowId < 2000) {
        val a = if (rng.nextInt(10) < 3) "hot" else "a" + rng.nextInt(100)
        sim.insert(rowId, Array(v(a), v("b" + rng.nextInt(100))))
        rowId += 1
      }
      sim.drain()
      sim.assertPerKeyOrder()
      assert(parkingScheduler(named).hotDrainedRows > 0, "expected the hot-RAID guard to engage")
    } finally {
      named.close()
    }
  }

  test("livelock bound: adversarial two-RAID interleavings terminate with bounded churn") {
    // Alternating P(a, b) / Q(b, a) rows over a small value set is the classic two-RAID bounce
    // shape. Tickets and the head-of-all-queues rule must keep re-admission work bounded.
    val values = Array("x", "y", "z", "w")
    val rows = mutable.ArrayBuffer.empty[Array[AnyValue]]
    var i = 0
    while (i < 4000) {
      val a = values(i % values.length)
      val b = values((i + 1) % values.length)
      rows += (if (i % 2 == 0) Array[AnyValue](v(a), v(b)) else Array[AnyValue](v(b), v(a)))
      i += 1
    }
    val named = parking(8, 2, 64)
    try {
      val sim = new SchedulerSimulator(named, 8, capacity = 8)
      rows.foreach(sim.insert)
      sim.drain()
      assert(sim.rowsIn == rows.length && sim.rowsOut == rows.length)
      val sched = parkingScheduler(named)
      assert(
        sched.reAdmissionChecks <= 8L * rows.length,
        s"re-admission churn exploded: ${sched.reAdmissionChecks} checks for ${rows.length} rows"
      )
    } finally {
      named.close()
    }
  }

  test("budget backpressure: insertRow returns BACKPRESSURE at the cap and the peak stays bounded") {
    val budget = 4
    val named = parking(1000, 2, 64, budget = budget)
    val sched = parkingScheduler(named)
    try {
      // Reserve "blocker" in a batch, close the underfilled batch via force poll and dispatch it,
      // so the value is owned by an in-flight batch
      val blockerBatch = sched.insertRow(Array(v("blocker"), v("b0")))
      assert(blockerBatch >= 0)
      val polled = sched.pollReadyBatch(force = true)
      assert(polled == blockerBatch)
      sched.dispatchBatch(polled)
      // Rows conflicting with the in-flight batch park, until the budget is exhausted
      var parked = 0
      var sawBackpressure = false
      var attempt = 0
      while (attempt < budget + 3 && !sawBackpressure) {
        val r = sched.insertRow(Array(v("blocker"), v("other" + attempt)))
        if (r <= BatchScheduler.PARKED_BASE) parked += 1
        else if (r == BatchScheduler.BACKPRESSURE) sawBackpressure = true
        else fail(s"row with a live blocked owner must park or backpressure, got $r")
        attempt += 1
      }
      assert(parked == budget, s"expected exactly $budget parked rows, got $parked")
      assert(sawBackpressure, "expected BACKPRESSURE once the parking budget was full")
      assert(sched.parkedPeak == budget)
    } finally {
      named.close()
    }
  }

  test("cross-word forced admission: a parked row drained into slot 64 waits for its slot-0 blocker (maxBatches=128)") {
    // maxBatches=128 gives ROW_WORDS=2. batchSize=1 seals every batch on its first row, so slots
    // 0..63 fill word 0 and the forced placement of the parked row must allocate slot 64 in word 1
    // with a dependency edge back to slot 0.
    val named = parking(bs = 1, rpr = 1, mb = 128)
    try {
      val sched = named.scheduler
      sched.insertRow(Array(v("shared"))) shouldBe 0
      var i = 1
      while (i < 64) {
        sched.insertRow(Array(v("k" + i))) shouldBe i
        i += 1
      }
      // The second "shared" row is blocked by the sealed batch 0, so it parks
      val parked = sched.insertRow(Array(v("shared")))
      parked should be <= BatchScheduler.PARKED_BASE
      val slot = BatchScheduler.parkedSlot(parked)

      val dispatched = mutable.ArrayBuffer.empty[Int]
      var d = sched.pollReadyBatch(force = false)
      while (d >= 0) {
        sched.dispatchBatch(d)
        dispatched += d
        d = sched.pollReadyBatch(force = false)
      }
      dispatched.toSet shouldBe (0 until 64).toSet

      // Forced admission places the parked row with dependency edges: slot 64, depending on slot 0
      val placements = mutable.ArrayBuffer.empty[(Int, Int)]
      val sink: ParkedRowSink = new ParkedRowSink {
        override def place(parkedSlot: Int, batchIdx: Int): Unit = placements += ((parkedSlot, batchIdx))
      }
      sched.admitParked(force = true, sink)
      placements.toSeq shouldBe Seq((slot, 64))
      sched.hasParkedRows shouldBe false
      // Batch 64 is blocked on the in-flight batch 0, not merely un-full
      sched.pollReadyBatch(force = true) shouldBe -1

      sched.completeBatch(0)
      val released = sched.pollReadyBatch(force = false)
      released shouldBe 64
      sched.dispatchBatch(released)
      sched.completeBatch(released)
    } finally {
      named.close()
    }
  }

  test("drain with parked rows: input ending while rows are parked loses nothing") {
    // Small circulant, batch size larger than per-value row count, so late same-value rows park
    // behind full batches and are still parked when input ends
    val rows = shuffledCirculant(m = 50, d = 8, seed = 5L)
    val named = parking(16, 2, 64)
    try {
      val sim = new SchedulerSimulator(named, 16, capacity = 4)
      rows.foreach(sim.insert)
      sim.drain()
      assert(sim.rowsIn == rows.length && sim.rowsOut == rows.length)
      assert(sim.parkedCount == 0)
      assert(!named.scheduler.hasParkedRows)
    } finally {
      named.close()
    }
  }

  test("close with parked rows releases all scheduler heap") {
    val memoryTracker = new LocalMemoryTracker()
    val sched = new ParkingBitmaskBatchScheduler(4, 2, 64, 16, memoryTracker)
    // Reserve values in one batch, close it via force poll and dispatch, then park rows against it
    assert(sched.insertRow(Array(v("k1"), v("k2"))) >= 0)
    val idx = sched.pollReadyBatch(force = true)
    assert(idx >= 0)
    sched.dispatchBatch(idx)
    assert(sched.insertRow(Array(v("k1"), v("x1"))) <= BatchScheduler.PARKED_BASE)
    assert(sched.insertRow(Array(v("k2"), v("x2"))) <= BatchScheduler.PARKED_BASE)
    assert(sched.hasParkedRows)
    sched.close()
    assert(
      memoryTracker.estimatedHeapMemory() == 0,
      s"scheduler heap not fully released: ${memoryTracker.estimatedHeapMemory()} bytes"
    )
  }

  test("a row with duplicate RAID values does not block itself") {
    val named = parking(4, 2, 64)
    try {
      val sim = new SchedulerSimulator(named, 4, capacity = 4)
      var i = 0
      while (i < 100) {
        sim.insert(Array(v("dup" + (i % 5)), v("dup" + (i % 5))))
        i += 1
      }
      sim.drain()
      assert(sim.rowsIn == 100 && sim.rowsOut == 100)
    } finally {
      named.close()
    }
  }

  test("one completion strikes a still-blocked parked row at most once despite duplicate wakes") {
    // Row E parks on (r1, r2, r3) with r1 and r2 owned by one in-flight batch and r3 by another.
    // Completing the first batch wakes both r1 and r2 (two wake entries, one completion) while E
    // stays blocked by the second batch: E must accrue one strike, not two, so with strikeLimit = 2
    // it is admitted zero-edge after the second completion instead of being force-placed with
    // dependency edges.
    val named = parking(1, 3, 64, budget = 16, strikeLimit = 2)
    val sched = parkingScheduler(named)
    try {
      val placed = mutable.ArrayBuffer.empty[(Int, Int)]
      val sink: ParkedRowSink = new ParkedRowSink {
        override def place(parkedSlot: Int, batchIdx: Int): Unit = placed += ((parkedSlot, batchIdx))
      }

      val b0 = sched.insertRow(Array(v("r1"), v("r2"), v("x")))
      assert(b0 >= 0)
      assert(sched.pollReadyBatch(force = false) == b0)
      sched.dispatchBatch(b0)
      val b1 = sched.insertRow(Array(v("r3"), v("y"), v("z")))
      assert(b1 >= 0)
      assert(sched.pollReadyBatch(force = false) == b1)
      sched.dispatchBatch(b1)
      assert(sched.insertRow(Array(v("r1"), v("r2"), v("r3"))) <= BatchScheduler.PARKED_BASE)

      sched.completeBatch(b0)
      sched.admitParked(force = false, sink)
      // Still parked: r3's owner is in flight, and the duplicate wake must not have struck E out
      assert(sched.parkedCount == 1)
      assert(sched.strikeouts == 0L, "duplicate wakes from one completion burned several strikes")

      sched.completeBatch(b1)
      sched.admitParked(force = false, sink)
      assert(sched.parkedCount == 0)
      assert(sched.strikeouts == 0L)
      assert(sched.forcePlacedRows == 0L, "expected a zero-edge admission, not a forced placement")
      assert(placed.size == 1)
    } finally {
      named.close()
    }
  }

  test("waiters on one RAID admit in park order across the lone-entry and queue representations") {
    // First waiter is stored as the entry itself, the second promotes to a deque: admission after
    // the blocker completes must preserve park order through both representations
    val named = parking(2, 1, 64, budget = 16)
    val sched = parkingScheduler(named)
    try {
      val placed = mutable.ArrayBuffer.empty[(Int, Int)]
      val sink: ParkedRowSink = new ParkedRowSink {
        override def place(parkedSlot: Int, batchIdx: Int): Unit = placed += ((parkedSlot, batchIdx))
      }

      val b0 = sched.insertRow(Array(v("r1")))
      assert(b0 >= 0)
      assert(sched.insertRow(Array(v("r1"))) == b0) // self-absorbs and fills the batch
      assert(sched.pollReadyBatch(force = false) == b0)
      sched.dispatchBatch(b0)

      val resB = sched.insertRow(Array(v("r1")))
      assert(resB <= BatchScheduler.PARKED_BASE) // lone waiter, stored without a queue
      val resC = sched.insertRow(Array(v("r1")))
      assert(resC <= BatchScheduler.PARKED_BASE) // second waiter, promotes to a deque
      val slotB = BatchScheduler.parkedSlot(resB)
      val slotC = BatchScheduler.parkedSlot(resC)

      sched.completeBatch(b0)
      sched.admitParked(force = false, sink)

      assert(placed.map(_._1) == mutable.ArrayBuffer(slotB, slotC), "admission must preserve park order")
      assert(placed(0)._2 == placed(1)._2, "cascading admission should pack both rows into one batch")
      assert(sched.parkedCount == 0)
      assert(!sched.hasParkedRows)
    } finally {
      named.close()
    }
  }

  test("advantage: parking outruns affinity on scattered sparse conflicts") {
    // m > maxBatches * batchSize (the healthy-regime law), scattered order, sparse conflicts.
    // Affinity serializes through dependency contagion here, parking should stay shallow.
    val rows = shuffledCirculant(m = 1000, d = 20, seed = 11L) // 20000 rows, window = 64 * 10 = 640
    val affinityResult = {
      val named = affinity(10, 2, 64)
      try makespan(named, rows)
      finally named.close()
    }
    val parkingResult = {
      val named = parking(10, 2, 64, budget = 1280)
      try makespan(named, rows)
      finally named.close()
    }
    withClue(s"affinity=$affinityResult parking=$parkingResult: ") {
      assert(
        parkingResult._1 * 2 <= affinityResult._1,
        s"expected parking DAG depth (${parkingResult._1}) to be at most half of affinity's (${affinityResult._1})"
      )
    }
  }

  test("parity: parking does not regress in the true collapse regime") {
    // m well below the reservation window: every policy serializes, parking must not be
    // meaningfully worse than affinity
    val rows = shuffledCirculant(m = 100, d = 100, seed = 13L) // 10000 rows, window = 64 * 10 = 640
    val affinityResult = {
      val named = affinity(10, 2, 64)
      try makespan(named, rows)
      finally named.close()
    }
    val parkingResult = {
      val named = parking(10, 2, 64)
      try makespan(named, rows)
      finally named.close()
    }
    withClue(s"affinity=$affinityResult parking=$parkingResult: ") {
      assert(
        parkingResult._1 <= affinityResult._1 * 3 / 2,
        s"parking DAG depth (${parkingResult._1}) regressed more than 1.5x vs affinity (${affinityResult._1})"
      )
    }
  }
}
