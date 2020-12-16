/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.runtime.BufferInputStream
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingProbe
import org.neo4j.cypher.internal.runtime.spec.RecordingRowsProbe
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.ThreadSafeRecordingProbe
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values

import scala.collection.GenTraversable

abstract class EagerTestBase[CONTEXT <: RuntimeContext](
                                                              edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int
                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should exhaust input - single top-level eager") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = (0L until nBatches*batchSize).map(i => Array(Values.longValue(i)))
    assertRows(expected, subscriber.allSeen)
  }

  test("should exhaust input - single top-level eager with middle operator") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .projection("x as y")
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = (0L until nBatches*batchSize).map(i => Array(Values.longValue(i)))
    assertRows(expected, subscriber.allSeen)
  }

  test("should exhaust input - single top-level eager before limit") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(20)
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = (0L until 20).map(i => Array(Values.longValue(i)))
    assertRows(expected, subscriber.allSeen, hasLimit = true)
  }

  test("should exhaust input - single top-level eager after limit") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val probe = recordingProbe("x")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .prober(probe)
      .limit(20)
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe true
    val expected = (0L until 20).map(i => Array(Values.longValue(i)))
    assertRows(expected, probe.seenRows, hasLimit = true)

    result.request(Long.MaxValue)
    result.await() shouldBe false

    assertRows(expected, subscriber.allSeen, hasLimit = true)
  }

  test("should exhaust input - top-level eager with grouping aggregation") {
    val batchSize = 10
    val nBatches = Math.max(sizeHint / batchSize, 2)
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("g", "c")
      .aggregation(Seq(s"x / $nBatches AS g"), Seq("count(x) as c"))
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = (0L until batchSize).map(i => Array(Values.longValue(i), Values.longValue(nBatches)))
    assertRows(expected, subscriber.allSeen)
  }

  test("should exhaust input - two chained top-level eagers") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .eager()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    subscriber.numberOfSeenResults shouldEqual(nBatches * batchSize)

    val expected = (0L until nBatches*batchSize).map(i => Array(Values.longValue(i)))
    assertRows(expected, subscriber.allSeen)
  }

  test("should exhaust input - two chained top-level eagers with middle operators") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("z")
      .projection("y as z")
      .eager()
      .projection("x as y")
      .eager()
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    subscriber.numberOfSeenResults shouldEqual(nBatches * batchSize)

    val expected = (0L until nBatches*batchSize).map(i => Array(Values.longValue(i)))
    assertRows(expected, subscriber.allSeen)
  }

  test("single eager on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  // TODO: Investigate why this is slow in parallel
  test("single top-level eager on top of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .eager()
      .prober(probe)
      .apply()
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe false

    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = for {
      i <- 0L until nBatches * batchSize
      j <- 1L to rowsPerArgument.toLong
    } yield Array(Values.longValue(i), Values.longValue(j))

    assertRows(expected, subscriber.allSeen)
  }

  test("single eager with middle operator on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.projection("x AS x", "i AS i")
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  test("two chained eagers on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.projection("x AS x", "i AS i")
      .|.eager()
      .|.projection("x AS x", "i AS i")
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  test("two chained eagers with middle operators on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.eager()
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  test("single eager -> reduce on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.sort(Seq(Ascending("x"), Ascending("i")))
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  test("single reduce -> eager on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.eager()
      .|.prober(probe)
      .|.sort(Seq(Ascending("x"), Ascending("i")))
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize)
  }

  test("single eager -> limit on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.limit(rowsPerArgument/2)
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument, nBatches, batchSize, limit = Some(rowsPerArgument/2))
  }

  test("single limit -> eager on rhs of apply") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, Values.longValue(_)).stream()
    val rowsPerArgument = 10

    val probe = recordingProbe("x", "i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .apply()
      .|.eager()
      .|.prober(probe)
      .|.limit(rowsPerArgument/2)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument/2, nBatches, batchSize)
  }

  private def recordingProbe(variablesToRecord: String*): Prober.Probe with RecordingRowsProbe = {
    if (isParallel)
      RecordingProbe(variablesToRecord: _*)
    else
      ThreadSafeRecordingProbe(variablesToRecord: _*)
  }

  private def assertRows(expected: GenTraversable[_], actual: Seq[_], hasLimit: Boolean = false): Any = {
    if (isParallel) {
      if (hasLimit)
        actual.size shouldEqual expected.size
      else
        actual should contain theSameElementsAs expected
    } else {
      actual should contain theSameElementsInOrderAs expected
    }
  }

  private def assertPerArgumentEager(result: RuntimeResult,
                                     subscriber: TestSubscriber,
                                     inputStream: BufferInputStream,
                                     probe: RecordingRowsProbe,
                                     rowsPerArgument: Int,
                                     nBatches: Int,
                                     batchSize: Int,
                                     limit: Option[Int] = None): Unit = {
    // Pull one row
    result.request(1)
    result.await() shouldBe true
    runtimeTestSupport.waitForWorkersToIdle(1000)

    if (!isParallel) {
      inputStream.hasMore shouldBe true // But in parallel it may be possible to pre-buffer everything
    }

    // Check that at least the first 10 rows (the first argument) have been buffered
    // (with fusing it is possible more rows will be buffered)
    val expected0: Array[Array[LongValue]] = (1L to rowsPerArgument.toLong).map(i => Array(Values.longValue(0), Values.longValue(i))).toArray

    val count = probe.seenRows.size
    assertRows(expected0, probe.seenRows.take(rowsPerArgument), hasLimit = true)

    // Now we should be able to take more of the buffered rows without causing more new rows to be buffered
    // (exactly how many depends on runtime and plan, but 1 should work for all test cases)
    result.request(1)
    result.await() shouldBe true
    runtimeTestSupport.waitForWorkersToIdle(1000)

    if (!isParallel) {
      probe.seenRows.size shouldBe count // But in parallel it may be possible to pre-buffer more, so we cannot guarantee this condition
    }

    // Pull the remaining
    result.request(Long.MaxValue)
    result.await() shouldBe false

    val expected = for {
      i <- 0L until nBatches * batchSize
      j <- 1L to Math.min(rowsPerArgument.toLong, limit.getOrElse(rowsPerArgument).toLong)
    } yield Array(Values.longValue(i), Values.longValue(j))

    assertRows(expected, subscriber.allSeen, hasLimit = true)
  }
}
