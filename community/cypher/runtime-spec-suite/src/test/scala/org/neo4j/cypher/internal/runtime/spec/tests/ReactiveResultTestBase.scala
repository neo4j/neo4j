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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues

import java.io.IOException

import scala.collection.mutable

abstract class ReactiveResultTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should handle requesting one record at a time") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array("1", 1), Array("2", 2), Array("3", 3))

    // request 1
    result.request(1)
    result.await() shouldBe true
    val allSeen = mutable.Set(
      Seq[AnyValue](stringValue("1"), longValue(1)),
      Seq[AnyValue](stringValue("2"), longValue(2)),
      Seq[AnyValue](stringValue("3"), longValue(3))
    )
    if (!isParallel) {
      subscriber.lastSeen should equal(Seq[AnyValue](stringValue("1"), longValue(1)))
    }
    allSeen.remove(subscriber.lastSeen) shouldBe true
    subscriber.isCompleted shouldBe false

    // request 2
    result.request(1)
    result.await() shouldBe true
    if (!isParallel) {
      subscriber.lastSeen should equal(Seq[AnyValue](stringValue("2"), longValue(2)))
    }
    allSeen.remove(subscriber.lastSeen) shouldBe true
    subscriber.isCompleted shouldBe false

    // request 3
    result.request(1)
    val lastAwait = result.await()
    if (!isParallel) {
      subscriber.lastSeen should equal(Seq[AnyValue](stringValue("3"), longValue(3)))
    }
    allSeen.remove(subscriber.lastSeen) shouldBe true
    allSeen shouldBe empty

    // There is no more data so lastAwait should in that regard be false, however
    // we cannot guarantee that since we are also out of demand. In the next request-await cycle
    // we should in that case just return false though.
    if (lastAwait) {
      result.request(1)
      result.await() shouldBe false
      subscriber.isCompleted shouldBe true
    } else {
      subscriber.isCompleted shouldBe true
    }
  }

  test("should handle requesting more data than available") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // When
    result.request(17)

    // Then
    result.await() shouldBe false
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2)),
        List(longValue(3))
      )
    )
    subscriber.isCompleted shouldBe true
  }

  test("should handle requesting data multiple times") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // When
    result.request(1)
    result.request(1)

    // Then
    result.await() shouldBe true
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2))
      )
    )
    subscriber.isCompleted shouldBe false
  }

  test("should handle request overflowing the demand") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // When
    result.request(Int.MaxValue)
    result.request(Long.MaxValue)

    // Then
    result.await() shouldBe false
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2)),
        List(longValue(3))
      )
    )
    subscriber.isCompleted shouldBe true
  }

  test("should get expected value of hasServedRows") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // Then
    result.hasServedRows shouldBe false

    // When
    result.request(1)
    result.await()

    // Then
    result.hasServedRows shouldBe true
  }

  test("should handle cancel stream") {
    // Given
    val subscriber = TestSubscriber.concurrent
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // When
    result.request(1)
    result.await() shouldBe true
    if (isParallel) {
      // no guarantee of order
      Set(longValue(1), longValue(2), longValue(3)) should contain(subscriber.lastSeen.head)
    } else {
      subscriber.lastSeen should equal(Array(longValue(1)))
    }
    subscriber.isCompleted shouldBe false
    result.cancel()
    result.request(1)

    // Then
    result.await() shouldBe false
    subscriber.numberOfSeenResults shouldBe 1
    subscriber.isCompleted shouldBe false
  }

  test("should handle cancel stream and close cursors") {
    val nodes = givenGraph { nodeGraph(3) }

    val subscriber = TestSubscriber.concurrent
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime, subscriber)

    // When
    result.request(1)
    result.await() shouldBe true

    if (isParallel) {
      // No guarantees about order
      nodes.map(n => VirtualValues.node(n.getId)) should contain(subscriber.lastSeen.head)
    } else {
      subscriber.lastSeen should equal(Array(VirtualValues.node(nodes.head.getId)))
    }
    subscriber.isCompleted shouldBe false
    result.cancel()
    result.request(1)

    // Then
    result.await() shouldBe false
    subscriber.numberOfSeenResults shouldBe 1
    subscriber.isCompleted shouldBe false
  }

  test("should handle multiple times cancel stream and close cursors") {
    val nodes = givenGraph { nodeGraph(3) }

    val subscriber = TestSubscriber.concurrent
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime, subscriber)

    // When
    result.request(1)
    result.await() shouldBe true
    if (isParallel) {
      // No guarantees about order
      nodes.map(n => VirtualValues.node(n.getId)) should contain(subscriber.lastSeen.head)
    } else {
      subscriber.lastSeen should equal(Array(VirtualValues.node(nodes.head.getId)))
    }
    subscriber.isCompleted shouldBe false
    result.cancel()
    val now = System.currentTimeMillis()
    while (System.currentTimeMillis() - now < 1200) {
      // Bombard with cancel requests, so that we likely have multiple CleanupTasks in the parallel runtime
      result.cancel()
    }

    // Then
    result.await() shouldBe false
    subscriber.numberOfSeenResults shouldBe 1
    subscriber.isCompleted shouldBe false
  }

  test("should not exhaust input when there is no demand") {
    // NOTE: In parallel runtime with bigger morsel size and high worker count
    //       (=> larger buffer limits before backpressure is applied)
    //       we can get a flaky test if this is too low
    val nNodes = if (isParallel) 5000 else 1000
    val (nodes, _) = givenGraph { circleGraph(nNodes) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-->(y)")
      .input(nodes = Seq("x"))
      .build()
    val stream = batchedInputValues(1, nodes.map(Array[Any](_)): _*).stream()

    // When
    val result = execute(
      logicalQuery,
      runtime,
      stream,
      TestSubscriber.concurrent,
      testPlanCombinationRewriterHints = Set(TestPlanCombinationRewriter.NoEager)
    )

    // Then
    result.request(1)
    result.await() shouldBe true
    // we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true

    // However if we demand the rest of the data we should
    result.request(9999)
    result.await() shouldBe false
    stream.hasMore shouldBe false
  }

  test("should only call onResult once") {
    // Given
    val subscriber = mock[QuerySubscriber]
    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    // When
    result.request(1)
    result.await()
    result.request(1)
    result.await()
    result.request(1)
    result.await()

    // Then
    verify(subscriber, times(1)).onResult(1)
  }

  test("should not drop row because of limit") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(2)
      .input(variables = Seq("x"))
      .build()

    val input = inputValues(Array(1), Array(2)).stream()
    val subscriber = TestSubscriber.concurrent
    val result = execute(logicalQuery, runtime, input, subscriber)

    // when
    result.request(1)
    result.await()
    result.request(1)
    result.await()

    // then
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2))
      )
    )
  }

  test("should handle throwing subscriber") {
    val subscriber = mock[QuerySubscriber]
    val exception = new IOException("two is the loneliest number since the number one")
    when(subscriber.onField(0, intValue(2))).thenThrow(exception)

    val result = runtimeResult(subscriber, Array(1), Array(2), Array(3))

    result.request(1)
    result.await() shouldBe true
    verify(subscriber, never).onError(any[Throwable])

    result.request(1)
    an[IOException] shouldBe thrownBy(result.await())
    verify(subscriber).onError(exception)
  }

  private def runtimeResult(subscriber: QuerySubscriber, data: Array[Any]*): RuntimeResult = {
    val variables = (1 to data.head.length).map(i => s"v$i")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(variables: _*)
      .input(variables = variables)
      .build()

    execute(logicalQuery, runtime, inputValues(data: _*).stream(), subscriber)
  }
}
