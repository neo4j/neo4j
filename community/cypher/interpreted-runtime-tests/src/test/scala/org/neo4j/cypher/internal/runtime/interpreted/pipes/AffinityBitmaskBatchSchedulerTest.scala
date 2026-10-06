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

import org.neo4j.cypher.internal.runtime.interpreted.pipes.ParkingBitmaskBatchSchedulerTest.parking
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable
import scala.util.Random

object AffinityBitmaskBatchSchedulerTest {

  final case class NamedScheduler(name: String, scheduler: BatchScheduler) extends AutoCloseable {
    override def close(): Unit = scheduler.close()
  }

  def greedy(bs: Int, rpr: Int, mb: Int): NamedScheduler =
    NamedScheduler("greedy", new BitmaskBatchScheduler(bs, rpr, mb, EmptyMemoryTracker.INSTANCE))

  def affinity(bs: Int, rpr: Int, mb: Int): NamedScheduler =
    NamedScheduler("affinity", new AffinityBitmaskBatchScheduler(bs, rpr, mb, EmptyMemoryTracker.INSTANCE))

  /**
   * Full insert/dispatch/complete simulation with bounded in-flight capacity and FIFO completion.
   * Verifies on every dispatch that the dispatched batch's raid-value set is disjoint from every
   * in-flight batch's raid-value set (the safety invariant), and that no rows are lost.
   * Parking-aware: rows the scheduler parks are held aside and transferred to their batch when the
   * scheduler admits them through the sink.
   */
  final class SchedulerSimulator(named: NamedScheduler, batchSize: Int, capacity: Int) {
    private val sched = named.scheduler
    private def name = named.name

    var batchesFormed = 0
    var waitEvents = 0
    var forcedDispatches = 0
    var rowsIn = 0
    var rowsOut = 0
    val fills: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer.empty
    private val formationRaids = mutable.Map.empty[Int, mutable.Set[AnyValue]]
    private val formationRows = mutable.Map.empty[Int, Int]
    private val parkedBySlot = mutable.Map.empty[Int, Array[AnyValue]]
    private case class InFlight(idx: Int, raids: Set[AnyValue], rows: Int)
    private val inFlight = mutable.Queue.empty[InFlight]

    private val sink: ParkedRowSink = new ParkedRowSink {
      override def place(parkedSlot: Int, batchIdx: Int): Unit = {
        val raids = parkedBySlot.remove(parkedSlot)
          .getOrElse(throw new AssertionError(s"$name: sink placed unknown park slot $parkedSlot"))
        addToFormation(batchIdx, raids)
      }
    }

    private def addToFormation(idx: Int, raids: Array[AnyValue]): Unit = {
      formationRaids.getOrElseUpdate(idx, mutable.Set.empty[AnyValue]) ++= raids
      formationRows(idx) = formationRows.getOrElse(idx, 0) + 1
    }

    def parkedCount: Int = parkedBySlot.size

    def insert(raids: Array[AnyValue]): Unit = {
      var result = sched.insertRow(raids)
      while (result == BatchScheduler.BACKPRESSURE) {
        val d = sched.pollReadyBatch(force = false)
        if (d >= 0) {
          dispatch(d, forced = false)
        } else if (inFlight.nonEmpty) {
          waitEvents += 1
          completeOldest()
        } else {
          if (sched.hasParkedRows) {
            sched.admitParked(force = true, sink)
          }
          val f = sched.pollReadyBatch(force = true)
          assert(f >= 0, s"$name: liveness violation - backpressure, nothing pollable, nothing in flight")
          dispatch(f, forced = true)
          completeOldest()
        }
        result = sched.insertRow(raids)
      }
      rowsIn += 1
      if (result <= BatchScheduler.PARKED_BASE) {
        val slot = BatchScheduler.parkedSlot(result)
        assert(!parkedBySlot.contains(slot), s"$name: park slot $slot reused while occupied")
        parkedBySlot(slot) = raids
      } else {
        addToFormation(result, raids)
      }
      var d = sched.pollReadyBatch(force = false)
      while (d >= 0) {
        dispatch(d, forced = false)
        d = sched.pollReadyBatch(force = false)
      }
    }

    def drain(): Unit = {
      while (formationRows.nonEmpty || inFlight.nonEmpty || parkedBySlot.nonEmpty) {
        val d = sched.pollReadyBatch(force = false)
        if (d >= 0) {
          dispatch(d, forced = false)
        } else if (inFlight.nonEmpty) {
          waitEvents += 1
          completeOldest()
        } else {
          // Force admission may stop early on batch-slot exhaustion, so a force poll must follow
          // to dispatch a batch and free slots, exactly like the real iterator's drain path
          if (parkedBySlot.nonEmpty) {
            sched.admitParked(force = true, sink)
          }
          val f = sched.pollReadyBatch(force = true)
          if (f >= 0) {
            dispatch(f, forced = true)
          } else {
            assert(
              formationRows.isEmpty && parkedBySlot.isEmpty,
              s"$name: liveness violation in drain (${formationRows.size} forming, ${parkedBySlot.size} parked)"
            )
          }
        }
      }
    }

    private def dispatch(idx: Int, forced: Boolean): Unit = {
      val raids = formationRaids.remove(idx).map(_.toSet).getOrElse(Set.empty)
      val rows = formationRows.remove(idx).getOrElse(0)
      assert(rows > 0, s"$name: dispatched batch $idx with no rows")
      inFlight.foreach { f =>
        val overlap = f.raids.intersect(raids)
        assert(
          overlap.isEmpty,
          s"$name: SAFETY VIOLATION batch $idx overlaps in-flight batch ${f.idx} on values $overlap"
        )
      }
      sched.dispatchBatch(idx)
      batchesFormed += 1
      if (forced && rows < batchSize) forcedDispatches += 1
      fills += rows
      if (inFlight.size >= capacity) completeOldest()
      inFlight.enqueue(InFlight(idx, raids, rows))
    }

    private def completeOldest(): Unit = {
      val f = inFlight.dequeue()
      rowsOut += f.rows
      sched.completeBatch(f.idx)
      if (sched.hasParkedRows) {
        sched.admitParked(force = false, sink)
      }
    }
  }

  /**
   * Parallel makespan: time-stepped simulation with unlimited workers. At each step, all ready batches
   * are dispatched and all in-flight batches complete together. The number of steps approximates the
   * depth of the dependency DAG, i.e. wall-clock time with ample concurrency. Sensitive to dependency
   * contagion in a way that hard-stall counting is not.
   * @return (steps, batchesFormed)
   */
  def makespan(named: NamedScheduler, rows: Array[Array[AnyValue]]): (Int, Int) = {
    val sched = named.scheduler
    val inFlight = mutable.ArrayBuffer.empty[Int]
    var steps = 0
    var batches = 0

    val sink: ParkedRowSink = new ParkedRowSink {
      override def place(parkedSlot: Int, batchIdx: Int): Unit = {}
    }

    def dispatchAllReady(force: Boolean): Boolean = {
      var any = false
      var d = sched.pollReadyBatch(force)
      while (d >= 0) {
        sched.dispatchBatch(d)
        inFlight += d
        batches += 1
        any = true
        d = sched.pollReadyBatch(force = false)
      }
      any
    }

    def step(): Unit = {
      steps += 1
      inFlight.foreach(sched.completeBatch)
      inFlight.clear()
      if (sched.hasParkedRows) {
        sched.admitParked(force = false, sink)
      }
    }

    var i = 0
    while (i < rows.length) {
      var result = sched.insertRow(rows(i))
      while (result == BatchScheduler.BACKPRESSURE) {
        dispatchAllReady(force = false)
        if (inFlight.isEmpty) {
          if (sched.hasParkedRows) {
            sched.admitParked(force = true, sink)
          }
          val any = dispatchAllReady(force = true)
          assert(any, s"${named.name}: liveness violation in makespan simulation")
        }
        step()
        result = sched.insertRow(rows(i))
      }
      dispatchAllReady(force = false)
      i += 1
    }
    var remaining = true
    while (remaining) {
      if (sched.hasParkedRows) {
        sched.admitParked(force = true, sink)
      }
      dispatchAllReady(force = true)
      if (inFlight.nonEmpty) {
        step()
      } else {
        assert(!sched.hasParkedRows, s"${named.name}: parked rows remain with nothing in flight or ready")
        remaining = false
      }
    }
    (steps, batches)
  }
}

class AffinityBitmaskBatchSchedulerTest extends CypherFunSuite {
  import AffinityBitmaskBatchSchedulerTest._

  private def v(s: String): AnyValue = Values.stringValue(s)

  private def allSchedulers(bs: Int, rpr: Int, mb: Int): Seq[() => NamedScheduler] =
    Seq(() => greedy(bs, rpr, mb), () => affinity(bs, rpr, mb), () => parking(bs, rpr, mb))

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
      for (mk <- allSchedulers(batchSize, raidsPerRow, maxBatches)) {
        val named = mk()
        try {
          val sim = new SchedulerSimulator(named, batchSize, capacity = 8)
          rows.foreach(sim.insert)
          sim.drain()
          assert(
            sim.rowsIn == rows.length && sim.rowsOut == rows.length,
            s"${named.name}: lost rows in=${sim.rowsIn} out=${sim.rowsOut} " +
              s"(seed=$seed bs=$batchSize rpr=$raidsPerRow card=$cardinality mb=$maxBatches)"
          )
        } finally named.close()
      }
    }
  }

  test("contagion fix: A,A,A,B,B with batchSize=2 - B batch dispatches while batch 0 in flight") {
    def rows: Seq[Array[AnyValue]] = Seq(
      Array(v("id1"), v("A")),
      Array(v("id2"), v("A")),
      Array(v("id3"), v("A")),
      Array(v("id4"), v("B")),
      Array(v("id5"), v("B"))
    )
    for (mk <- Seq(() => affinity(2, 2, 64))) {
      val named = mk()
      try {
        val sched = named.scheduler
        rows.foreach(r => assert(sched.insertRow(r) >= 0))
        val first = sched.pollReadyBatch(force = false)
        assert(first >= 0, s"${named.name}: expected a filled+ready A batch")
        sched.dispatchBatch(first)
        // first is NOT completed: still in flight. The B batch must be dependency-free and ready now.
        val second = sched.pollReadyBatch(force = false)
        assert(second >= 0 && second != first, s"${named.name}: B batch should dispatch while batch 0 in flight")
        sched.dispatchBatch(second)
      } finally named.close()
    }
    // Greedy baseline on the same input: full serialization (dependency contagion)
    val g = greedy(2, 2, 64)
    try {
      rows.foreach(r => assert(g.scheduler.insertRow(r) >= 0))
      val first = g.scheduler.pollReadyBatch(force = false)
      assert(first >= 0)
      g.scheduler.dispatchBatch(first)
      assert(
        g.scheduler.pollReadyBatch(force = false) == -1,
        "greedy baseline: expected full serialization (contagion)"
      )
    } finally g.close()
  }

  test("quality metrics: 3 shapes x 3 schedulers, 5000 rows, batchSize 10, maxBatches 64, capacity 8") {
    val n = 5000
    val shapes: Seq[(String, () => Array[Array[AnyValue]])] = Seq(
      ("clustered", () => Array.tabulate(n)(i => Array[AnyValue](v("c" + i / 100), v("d" + i / 100)))),
      (
        "scattered-sparse",
        () => {
          val rng = new Random(7L)
          Array.tabulate(n)(i =>
            Array[AnyValue](v("u" + i), if (rng.nextDouble() < 0.05) v("hot" + rng.nextInt(10)) else v("w" + i))
          )
        }
      ),
      (
        "scattered-dense",
        () => {
          val rng = new Random(7L)
          Array.tabulate(n)(_ => Array[AnyValue](v("p" + rng.nextInt(20)), v("q" + rng.nextInt(20))))
        }
      )
    )
    val waitsByScheduler = mutable.Map.empty[(String, String), Int]
    val metrics = mutable.ArrayBuffer.empty[String]
    for {
      (shape, gen) <- shapes
      mk <- allSchedulers(10, 2, 64)
    } {
      val rows = gen()
      val named = mk()
      try {
        val sim = new SchedulerSimulator(named, batchSize = 10, capacity = 8)
        rows.foreach(sim.insert)
        sim.drain()
        assert(sim.rowsOut == n, s"${named.name}/$shape lost rows: ${sim.rowsOut}")
        waitsByScheduler((shape, named.name)) = sim.waitEvents
        val avgFill = sim.fills.sum.toDouble / sim.fills.size
        metrics += f"$shape/${named.name}: batches=${sim.batchesFormed} waits=${sim.waitEvents} " +
          f"forced=${sim.forcedDispatches} avgFill=$avgFill%.2f"
      } finally named.close()
    }
    // Sanity bound: on the scattered-sparse shape, affinity must not wait more than greedy.
    assert(
      waitsByScheduler(("scattered-sparse", "affinity")) <= waitsByScheduler(("scattered-sparse", "greedy")),
      s"affinity should not be worse than greedy on scattered-sparse:\n${metrics.mkString("\n")}"
    )
  }

  test("parallel makespan: affinity materially shortens the dependency DAG on scattered-sparse input") {
    val n = 5000
    val shapes: Seq[(String, () => Array[Array[AnyValue]])] = Seq(
      ("clustered", () => Array.tabulate(n)(i => Array[AnyValue](v("c" + i / 100), v("d" + i / 100)))),
      (
        "scattered-sparse",
        () => {
          val rng = new Random(7L)
          Array.tabulate(n)(i =>
            Array[AnyValue](v("u" + i), if (rng.nextDouble() < 0.05) v("hot" + rng.nextInt(10)) else v("w" + i))
          )
        }
      ),
      (
        "scattered-dense",
        () => {
          val rng = new Random(7L)
          Array.tabulate(n)(_ => Array[AnyValue](v("p" + rng.nextInt(20)), v("q" + rng.nextInt(20))))
        }
      )
    )
    val spans = mutable.Map.empty[(String, String), Int]
    val batchesFormed = mutable.Map.empty[(String, String), Int]
    for {
      (shape, gen) <- shapes
      mk <- allSchedulers(10, 2, 64)
    } {
      val named = mk()
      try {
        val (steps, batches) = makespan(named, gen())
        spans((shape, named.name)) = steps
        batchesFormed((shape, named.name)) = batches
      } finally named.close()
    }
    // The gate: with sparse scattered conflicts, affinity must materially shorten the critical path.
    val greedySparse = spans(("scattered-sparse", "greedy"))
    val affinitySparse = spans(("scattered-sparse", "affinity"))
    assert(
      affinitySparse * 2 <= greedySparse,
      s"expected affinity makespan to be at most half of greedy on scattered-sparse, " +
        s"got steps=$spans batches=$batchesFormed"
    )
  }

  test(
    "cross-word dependency: a word-1 batch depending on a word-0 batch is released by its completion (maxBatches=128)"
  ) {
    // maxBatches=128 gives ROW_WORDS=2. batchSize=1 seals every batch on its first row, so the ring
    // buffer allocates strictly increasing slots and the dependent row is forced into slot 64: its
    // newest writer (slot 0) is sealed and no open batch covers the dependency, so affinity allocates.
    val named = affinity(bs = 1, rpr = 1, mb = 128)
    try {
      val sched = named.scheduler
      sched.insertRow(Array(v("shared"))) shouldBe 0
      var i = 1
      while (i < 64) {
        sched.insertRow(Array(v("k" + i))) shouldBe i
        i += 1
      }
      // Records forwardDeps(64)(0) and reverseDeps(0)(1), an edge across the word boundary
      sched.insertRow(Array(v("shared"))) shouldBe 64

      val dispatched = mutable.ArrayBuffer.empty[Int]
      var d = sched.pollReadyBatch(force = false)
      while (d >= 0) {
        sched.dispatchBatch(d)
        dispatched += d
        d = sched.pollReadyBatch(force = false)
      }
      dispatched.toSet shouldBe (0 until 64).toSet
      // Batch 64 is blocked on the in-flight batch 0, not merely un-full
      sched.pollReadyBatch(force = true) shouldBe -1

      sched.completeBatch(0)
      val released = sched.pollReadyBatch(force = false)
      released shouldBe 64
      sched.dispatchBatch(released)
      sched.completeBatch(released)
    } finally named.close()
  }
}
