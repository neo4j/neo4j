/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.runtime.{IteratorBasedResult, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.{QueryProfile, QueryResult}
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{longValue, stringValue}

import scala.collection.mutable.ArrayBuffer

class PipeExecutionReactiveResultTest extends CypherFunSuite {

  test("should handle requesting one record at a time") {
    //Given
    val subscriber = new TestSubscriber
    val result = pipeResult(subscriber,
                            Array(stringValue("1"), longValue(1)),
                            Array(stringValue("2"), longValue(2)),
                            Array(stringValue("3"), longValue(3)))


    //request 1
    result.request(1)
    result.await() shouldBe true
    subscriber.lastSeen should equal(Seq[AnyValue](stringValue("1"), longValue(1)))
    subscriber.isCompleted shouldBe false

    //request 2
    result.request(1)
    result.await() shouldBe true
    subscriber.lastSeen should equal(Seq[AnyValue](stringValue("2"), longValue(2)))
    subscriber.isCompleted shouldBe false

    //request 3
    result.request(1)
    result.await() shouldBe false
    subscriber.lastSeen should equal(Seq[AnyValue](stringValue("3"), longValue(3)))
    subscriber.isCompleted shouldBe true
  }

  test("should handle requesting more data than available") {
    //Given
    val subscriber = new TestSubscriber
    val result = pipeResult(subscriber,
                            Array(longValue(1)),
                            Array(longValue(2)),
                            Array(longValue(3)))

    //When
    result.request(17)

    //Then
    result.await() shouldBe false
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2)),
        List(longValue(3)))
    )
    subscriber.isCompleted shouldBe true
  }

  test("should handle requesting data multiple times") {
    //Given
    val subscriber = new TestSubscriber
    val result = pipeResult(subscriber,
                            Array(longValue(1)),
                            Array(longValue(2)),
                            Array(longValue(3)))

    //When
    result.request(1)
    result.request(1)

    //Then
    result.await() shouldBe true
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2)))
    )
    subscriber.isCompleted shouldBe false
  }

  test("should handle request overflowing the demand") {
    //Given
    val subscriber = new TestSubscriber
    val result = pipeResult(subscriber,
                            Array(longValue(1)),
                            Array(longValue(2)),
                            Array(longValue(3)))

    //When
    result.request(Int.MaxValue)
    result.request(Long.MaxValue)

    //Then
    result.await() shouldBe false
    subscriber.allSeen should equal(
      List(
        List(longValue(1)),
        List(longValue(2)),
        List(longValue(3)))
    )
    subscriber.isCompleted shouldBe true
  }

  test("should handle cancel stream") {
    //Given
    val subscriber = new TestSubscriber
    val result = pipeResult(subscriber,
                            Array(longValue(1)),
                            Array(longValue(2)),
                            Array(longValue(3)))

    //When
    result.request(1)
    result.await() shouldBe true
    subscriber.lastSeen should equal(Array(longValue(1)))
    subscriber.isCompleted shouldBe false
    result.cancel()

    //Then
    result.await() shouldBe false
    subscriber.isCompleted shouldBe false
  }

  private def pipeResult(subscriber: QuerySubscriber, first: Array[AnyValue], more: Array[AnyValue]*) = {
    val fieldNames = (1 to first.length).map(i => s"f$i").toArray
    val results = (first +: more).iterator.map(new ResultRecord(_))
    new PipeExecutionResult(
      IteratorBasedResult(Iterator.empty, Some(results)),
      fieldNames, QueryStateHelper.emptyWith(query = mock[QueryContext](RETURNS_DEEP_STUBS)), QueryProfile.NONE, subscriber)
  }

  class TestSubscriber extends QuerySubscriber {

    private val records = ArrayBuffer.empty[List[AnyValue]]
    private var current: Array[AnyValue] = _
    private var done = false

    override def onResult(numberOfFields: Int): Unit = {
      current = new Array[AnyValue](numberOfFields)
    }

    override def onRecord(): Unit = {}

    override def onField(offset: Int, value: AnyValue): Unit = {
      current(offset) = value
    }

    override def onRecordCompleted(): Unit = {
      records.append(current.toList)
    }

    override def onError(throwable: Throwable): Unit = {

    }

    override def onResultCompleted(statistics: QueryStatistics): Unit = {
      done = true
    }


    def isCompleted: Boolean = done

    def lastSeen: Seq[AnyValue] = current

    //convert to list since nested array equality doesn't work nicely in tests
    def allSeen: Seq[Seq[AnyValue]] = records
  }

  class ResultRecord(val fields: Array[AnyValue]) extends QueryResult.Record
}
