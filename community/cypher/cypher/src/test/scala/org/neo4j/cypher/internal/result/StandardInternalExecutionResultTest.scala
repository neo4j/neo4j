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
package org.neo4j.cypher.internal.result

import java.io.PrintWriter
import java.lang
import java.util.Optional

import org.mockito.Mockito.{RETURNS_DEEP_STUBS, times, verify}
import org.neo4j.cypher.internal.InterpretedRuntimeName
import org.neo4j.cypher.internal.javacompat.ResultSubscriber
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.util.TaskCloser
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.kernel.impl.query.{QuerySubscriber, TransactionalContext}
import org.neo4j.values.storable.Values.intValue

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

  private def assertMaterializationAndCloseOfInit(queryType: InternalQueryType,
                                                  shouldMaterialize: Boolean,
                                                  shouldClose: Boolean,
                                                  inner: TestRuntimeResult = TestRuntimeResult(List(1))): Unit = {
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
    assertMaterializationOfMethod(false, false, TestRuntimeResult(List(1)), _.hasNext)
  }

  test("should not materialize iterable result when javaColumnAs") {
    assertMaterializationOfMethod(false, false, TestRuntimeResult(List(1)), _.columnAs[Int]("x").hasNext)
  }

  // DUMP TO STRING

  test("should not materialize when dumpToString I") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1)), _.resultAsString())
  }

  test("should not materialize when dumpToString II") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1)), _.writeAsStringTo(mock[PrintWriter]))
  }

  // ACCEPT

  test("should not materialize when accept I") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1)), _.accept(mock[ResultVisitor[Exception]]))
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1)), _.accept(mock[ResultVisitor[Exception]]))
  }

  private def assertMaterializationOfMethod(shouldMaterialize: Boolean,
                                            shouldExhaust: Boolean,
                                            inner: TestRuntimeResult = TestRuntimeResult(List(1)),
                                            f: Result => Unit): Unit = {
    // given
    val result = inner.subscriber
    val x = standardInternalExecutionResult(inner, READ_ONLY, result)
    x.initiate()
    result.init(x)

    // when
    f(result)

    // then
    result.isMaterialized should be(shouldMaterialize)
  }

  private def standardInternalExecutionResult(inner: RuntimeResult, queryType: InternalQueryType,
                                              subscriber: QuerySubscriber) =
    new StandardInternalExecutionResult(
      mock[QueryContext],
      InterpretedRuntimeName,
      inner,
      new TaskCloser,
      queryType,
      NormalMode,
      mock[PlanDescriptionBuilder],
      subscriber
      )

  case class TestRuntimeResult(values: Seq[Int],
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

    override def totalAllocatedMemory(): Optional[lang.Long] = Optional.empty()

    override def queryProfile(): QueryProfile = QueryProfile.NONE

    override def close(): Unit = {

    }

    override def request(numberOfRecords: Long): Unit = {
      demand += numberOfRecords
      if (demand < 0) {
        demand = Long.MaxValue
      }

      while (iterator.hasNext && served < demand && !cancelled) {
        subscriber.onRecord()
        val nextValue = intValue(iterator.next())
        for(_ <- 1 to numberOfFields) subscriber.onField(nextValue)
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
  }

}
