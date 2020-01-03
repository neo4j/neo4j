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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.InterpretedRuntimeName
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler.PlanDescriptionBuilder
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.cypher.internal.v3_5.util.TaskCloser
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

import scala.collection.JavaConverters._

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
    val x = standardInternalExecutionResult(inner, queryType)

    // when
    x.initiate()

    // then
    if (shouldMaterialize)
      inner.consumptionState should be(ConsumptionState.EXHAUSTED)
    else
      inner.consumptionState should be(ConsumptionState.NOT_STARTED)

    x.isMaterialized should be(shouldMaterialize)
    x.isClosed should be(shouldClose)
  }

  // ITERATE

  test("should not materialize iterable result when javaIterator") {
    assertMaterializationOfMethod(false, false, TestRuntimeResult(List(1), isIterable = true), _.javaIterator.hasNext)
  }

  test("should not materialize iterable result when javaColumnAs") {
    assertMaterializationOfMethod(false, false, TestRuntimeResult(List(1), isIterable = true), _.javaColumnAs[Int]("x").hasNext)
  }

  test("should materialize not iterable result when javaIterator") {
    assertMaterializationOfMethod(true, true, TestRuntimeResult(List(1), isIterable = false), _.javaIterator.hasNext)
  }

  test("should materialize not iterable result when javaColumnAs") {
    assertMaterializationOfMethod(true, true, TestRuntimeResult(List(1), isIterable = false), _.javaColumnAs[Int]("x").hasNext)
  }

  // DUMP TO STRING

  test("should not materialize when dumpToString I") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = true), _.dumpToString())
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = false), _.dumpToString())
  }

  test("should not materialize when dumpToString II") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = true), _.dumpToString(mock[PrintWriter]))
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = false), _.dumpToString(mock[PrintWriter]))
  }

  // ACCEPT

  test("should not materialize when accept I") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = true), _.accept(mock[ResultVisitor[Exception]]))
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = false), _.accept(mock[ResultVisitor[Exception]]))
  }

  test("should not materialize when accept II") {
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = true), _.accept(mock[QueryResult.QueryResultVisitor[Exception]]))
    assertMaterializationOfMethod(false, true, TestRuntimeResult(List(1), isIterable = false), _.accept(mock[QueryResult.QueryResultVisitor[Exception]]))
  }

  private def assertMaterializationOfMethod(shouldMaterialize: Boolean,
                                            shouldExhaust: Boolean,
                                            inner: TestRuntimeResult = TestRuntimeResult(List(1)),
                                            f: InternalExecutionResult => Unit): Unit = {
    // given
    val x = standardInternalExecutionResult(inner, READ_ONLY)
    x.initiate()

    // when
    f(x)

    // then
    if (shouldExhaust)
      inner.consumptionState should be(ConsumptionState.EXHAUSTED)
    else
      inner.consumptionState should not be ConsumptionState.EXHAUSTED

    x.isMaterialized should be(shouldMaterialize)
  }

  private def standardInternalExecutionResult(inner: RuntimeResult, queryType: InternalQueryType) =
    new StandardInternalExecutionResult(
      mock[QueryContext],
      InterpretedRuntimeName,
      inner,
      new TaskCloser,
      queryType,
      NormalMode,
      mock[PlanDescriptionBuilder]
    )

  case class TestRuntimeResult(values: Seq[Int],
                               isIterable: Boolean = true,
                               var resultRequested: Boolean = false,
                               override val fieldNames: Array[String] = Array("x", "y")
                              ) extends RuntimeResult {

    private val iterator = values.iterator

    override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] = {
      resultRequested = true
      new ResourceIterator[util.Map[String, AnyRef]] {

        override def close(): Unit = {}

        override def hasNext: Boolean = iterator.hasNext

        override def next(): util.Map[String, AnyRef] = {
          val value = iterator.next()
          fieldNames.map(key => (key,value.asInstanceOf[AnyRef])).toMap.asJava
        }
      }
    }

    override def consumptionState: RuntimeResult.ConsumptionState =
      if (!resultRequested) ConsumptionState.NOT_STARTED
      else if (iterator.hasNext) ConsumptionState.HAS_MORE
      else ConsumptionState.EXHAUSTED

    override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit = {
      resultRequested = true
      while (iterator.hasNext) {
        val value = Values.of(iterator.next())
        val record = new QueryResult.Record {
          override def fields(): Array[AnyValue] = Array().padTo(fieldNames.length, value)
        }
        visitor.visit(record)
      }
    }

    override def queryStatistics(): QueryStatistics = QueryStatistics()

    override def queryProfile(): QueryProfile = QueryProfile.NONE

    override def close(): Unit = {
      resultRequested = true
      while (iterator.hasNext)
        iterator.next()
    }
  }
}
