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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.runtime.BufferInputStream
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRowsProbe
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue

import scala.collection.GenTraversable

abstract class EagerTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime,
      testPlanCombinationRewriterHints = Set(TestPlanCombinationRewriter.NoEager)
    ) {

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

    val expected = (0L until nBatches * batchSize).map(i => Array(Values.longValue(i)))
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

    val expected = (0L until nBatches * batchSize).map(i => Array(Values.longValue(i)))
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

    if (!isParallel) {
      // depending on fusing & morsel size & nBatches parallel runtime may exhaust input before cancellation kicks in
      inputStream.hasMore shouldBe true
    }
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

    subscriber.numberOfSeenResults shouldEqual (nBatches * batchSize)

    val expected = (0L until nBatches * batchSize).map(i => Array(Values.longValue(i)))
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

    subscriber.numberOfSeenResults shouldEqual (nBatches * batchSize)

    val expected = (0L until nBatches * batchSize).map(i => Array(Values.longValue(i)))
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

  test("two chained eagers with middle operators on rhs of apply") {
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

  test("two chained eagers on rhs of apply") {
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
      .|.sort("x ASC", "i ASC")
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
      .|.sort("x ASC", "i ASC")
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
      .|.limit(rowsPerArgument / 2)
      .|.eager()
      .|.prober(probe)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(
      result,
      subscriber,
      inputStream,
      probe,
      rowsPerArgument,
      nBatches,
      batchSize,
      limit = Some(rowsPerArgument / 2)
    )
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
      .|.limit(rowsPerArgument / 2)
      .|.unwind(s"range(1,$rowsPerArgument) as i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, inputStream, subscriber)

    assertPerArgumentEager(result, subscriber, inputStream, probe, rowsPerArgument / 2, nBatches, batchSize)
  }

  test("eager with empty result") {
    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .eager()
      .emptyResult()
      .eager()
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream)
    result should beColumns().withNoRows()
  }

  test("eager with create and empty result") {
    assume(!isParallel)

    val nBatches = Math.max(sizeHint / 10, 2)
    val batchSize = 10
    val inputStream = inputColumns(nBatches, batchSize, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .eager()
      .emptyResult()
      .eager()
      .create(createNode("n"))
      .eager()
      .input(variables = Seq("x"))
      .build(readOnly = false)

    val result = execute(logicalQuery, runtime, inputStream)
    result should beColumns().withNoRows().withStatistics(nodesCreated = nBatches * batchSize)
  }

  // ----------------------------------------------------------------------------
  // Tests with varying number of input rows
  // ----------------------------------------------------------------------------

  List(0, 1, sizeHint / 2).foreach { nInputRows =>
    val expected: Array[Array[Long]] = (0L until nInputRows).map(Array(_)).toArray

    test(s"two top-level eagers with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .eager()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"three top-level eagers with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .eager()
        .eager()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"two eagers on rhs of apply with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .apply()
        .|.eager()
        .|.eager()
        .|.argument()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"eager on rhs of apply and eager on top with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .apply()
        .|.eager()
        .|.argument()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"eager on rhs of apply and two eager on top with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .eager()
        .apply()
        .|.eager()
        .|.argument()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"eager + eager on rhs of apply + eager on top with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .apply()
        .|.eager()
        .|.argument()
        .eager()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"eager + nested eager on rhs of apply + eager on top with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .eager()
        .apply()
        .|.apply()
        .|.|.eager()
        .|.|.argument()
        .|.eager()
        .|.argument()
        .eager()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }

    test(s"eager with middle operator + nested eager on rhs of apply + eager on top with $nInputRows input rows") {
      val inputStream = inputColumns(1, nInputRows, Values.longValue(_)).stream()

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .projection("x AS x")
        .eager()
        .apply()
        .|.apply()
        .|.|.projection("x AS x")
        .|.|.eager()
        .|.|.argument()
        .|.projection("x AS x")
        .|.eager()
        .|.argument()
        .projection("x AS x")
        .eager()
        .input(variables = Seq("x"))
        .build()

      val result = execute(logicalQuery, runtime, inputStream)
      result should beColumns("x").withRows(expected)
    }
  }

  test("should discard columns") {
    assume(runtime.name != "interpreted")

    val probe1 = recordingProbe("keep", "discard")
    val probe2 = recordingProbe("keep", "discard")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("keep")
      .prober(probe2)
      .nonFuseable() // Needed because of limitation in prober
      // keep is discarded here but should not be removed since we don't put it in an eager buffer
      .projection("keep + 'done' as hi")
      .eager()
      .prober(probe1)
      .projection("keep as keep")
      .projection("'bla' + a as keep", "'blö' + a as discard")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)
    result should beColumns("keep")
      .withRows(Range.inclusive(0, sizeHint).map(i => Array(s"bla$i")))

    probe1.seenRows.map(_.toSeq).toSeq should contain theSameElementsAs
      Range.inclusive(0, sizeHint).map(i => Seq(stringValue(s"bla$i"), stringValue(s"blö$i")))

    probe2.seenRows.map(_.toSeq).toSeq should contain theSameElementsAs
      Range.inclusive(0, sizeHint).map(i => Seq(stringValue(s"bla$i"), null))
  }

  test("should discard columns under apply") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "c")
      .apply()
      .|.eager()
      .|.projection("b+1 AS c", "b+2 AS d")
      .|.argument("b")
      .projection("a+1 AS b", "a+2 AS c")
      .unwind(s"range(0, $sizeHint) AS a")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    result should beColumns("a", "c")
      .withRows(Range.inclusive(0, sizeHint).map(i => Array[Any](i, i + 2)))
  }

  protected def assertRows(expected: GenTraversable[_], actual: Seq[_], hasLimit: Boolean = false): Any = {
    if (isParallel) {
      if (hasLimit)
        actual.size shouldEqual expected.size
      else
        actual should contain theSameElementsAs expected
    } else {
      actual should contain theSameElementsInOrderAs expected
    }
  }

  protected def assertPerArgumentEager(
    result: RuntimeResult,
    subscriber: TestSubscriber,
    inputStream: BufferInputStream,
    probe: RecordingRowsProbe,
    rowsPerArgument: Int,
    nBatches: Int,
    batchSize: Int,
    limit: Option[Int] = None
  ): Unit = {
    // Pull one row
    result.request(1)
    result.await() shouldBe true
    runtimeTestSupport.waitForWorkersToIdle(1000)

    if (!isParallel) {
      inputStream.hasMore shouldBe true // But in parallel it may be possible to pre-buffer everything
    }

    // Check that at least the first 10 rows (the first argument) have been buffered
    // (with fusing it is possible more rows will be buffered)
    val expected0: Array[Array[LongValue]] =
      (1L to rowsPerArgument.toLong).map(i => Array(Values.longValue(0), Values.longValue(i))).toArray

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
