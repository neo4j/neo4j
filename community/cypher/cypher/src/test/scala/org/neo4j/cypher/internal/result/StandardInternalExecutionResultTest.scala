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
package org.neo4j.cypher.internal.result

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.neo4j.cypher.internal.javacompat.ResultSubscriber
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.result.StandardInternalExecutionResult.NoOuterCloseable
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.READ_ONLY
import org.neo4j.cypher.internal.runtime.READ_WRITE
import org.neo4j.cypher.internal.runtime.SCHEMA_WRITE
import org.neo4j.cypher.internal.runtime.WRITE
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.storable.Values.intValue

import java.io.PrintWriter
import java.util
import java.util.Collections

//noinspection NameBooleanParameters,RedundantDefaultArgument
class StandardInternalExecutionResultTest extends CypherFunSuite {

  // INITIATE

  test("should not materialize or close read-only result") {
    assertMaterializationAndCloseOfInit(READ_ONLY, false, false)
  }

  test("should materialize but not close read-write result") {
    assertMaterializationAndCloseOfInit(READ_WRITE, true, false)
  }

  test("should materialize and close write-only result") {
    assertMaterializationAndCloseOfInit(WRITE, true, true)
  }

  test("should materialize and close write-only result for all QuerySubscribers") {
    // given
    val runtimeResult = mock[RuntimeResult]
    val subscriber = mock[QuerySubscriber]
    val x = standardInternalExecutionResult(runtimeResult, WRITE, subscriber)

    // when
    x.initiate()

    // then
    verify(runtimeResult, times(1)).request(1)
    verify(runtimeResult, times(1)).await()
    x.isClosed should be(true)
  }

  test("should close pre-exhausted schema-write result") {
    assertMaterializationAndCloseOfInit(SCHEMA_WRITE, true, true, TestRuntimeResult(Nil, resultRequested = true))
  }

  test("should consume and close result without field names") {
    assertMaterializationAndCloseOfInit(READ_WRITE, true, true, TestRuntimeResult(Nil, fieldNames = Array()))
  }

  private def assertMaterializationAndCloseOfInit(
    queryType: InternalQueryType,
    shouldMaterialize: Boolean,
    shouldClose: Boolean,
    inner: TestRuntimeResult = TestRuntimeResult(List(1))
  ): Unit = {
    // given
    // given
    val result = inner.subscriber
    val x = standardInternalExecutionResult(inner, queryType, result)

    x.initiate()
    result.init(x)

    // then
    result.isMaterialized should be(shouldMaterialize)
    x.isClosed should be(shouldClose)
  }

  // ITERATE

  test("should not materialize iterable result when javaIterator") {
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.hasNext)
  }

  test("should not materialize iterable result when javaColumnAs") {
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.columnAs[Int]("x").hasNext)
  }

  // DUMP TO STRING

  test("should not materialize when dumpToString I") {
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.resultAsString())
  }

  test("should not materialize when dumpToString II") {
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.writeAsStringTo(mock[PrintWriter]))
  }

  // ACCEPT

  test("should not materialize when accept I") {
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.accept(mock[ResultVisitor[Exception]]))
    assertMaterializationOfMethod(TestRuntimeResult(List(1)), _.accept(mock[ResultVisitor[Exception]]))
  }

  private def assertMaterializationOfMethod(inner: TestRuntimeResult, f: Result => Unit): Unit = {
    // given
    val result = inner.subscriber
    val x = standardInternalExecutionResult(inner, READ_ONLY, result)
    x.initiate()
    result.init(x)

    // when
    f(result)

    // then
    result.isMaterialized should be(false)
  }

  private def standardInternalExecutionResult(
    inner: RuntimeResult,
    queryType: InternalQueryType,
    subscriber: QuerySubscriber
  ) =
    new StandardInternalExecutionResult(
      inner,
      new TaskCloser,
      NoOuterCloseable,
      queryType,
      NormalMode,
      mock[PlanDescriptionBuilder],
      subscriber,
      Seq.empty,
      CancellationChecker.neverCancelled()
    )

  case class TestRuntimeResult(
    values: Seq[Int],
    var resultRequested: Boolean = false,
    override val fieldNames: Array[String] = Array("x", "y")
  ) extends RuntimeResult {

    val subscriber = new ResultSubscriber(mock[TransactionalContext](RETURNS_DEEP_STUBS))
    private val iterator = values.iterator
    private var demand = 0L
    private var served = 0L
    private var cancelled = false
    private val numberOfFields = fieldNames.length
    subscriber.onResult(numberOfFields)

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (!resultRequested) ConsumptionState.NOT_STARTED
      else if (iterator.hasNext) ConsumptionState.HAS_MORE
      else ConsumptionState.EXHAUSTED

    override def queryStatistics(): QueryStatistics = QueryStatistics()

    override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

    override def queryProfile(): QueryProfile = QueryProfile.NONE

    override def close(): Unit = {}

    override def request(numberOfRecords: Long): Unit = {
      demand += numberOfRecords
      if (demand < 0) {
        demand = Long.MaxValue
      }

      while (iterator.hasNext && served < demand && !cancelled) {
        subscriber.onRecord()
        val nextValue = intValue(iterator.next())
        for (i <- 0 until numberOfFields) subscriber.onField(i, nextValue)
        subscriber.onRecordCompleted()
        served += 1L
      }

      if (!iterator.hasNext) {
        subscriber.onResultCompleted(queryStatistics())
      }
    }

    override def cancel(): Unit = {
      cancelled = true
    }

    override def await(): Boolean = iterator.hasNext

    override def notifications(): util.Set[InternalNotification] = Collections.emptySet()

    override def getErrorOrNull: Throwable = null
  }

}
