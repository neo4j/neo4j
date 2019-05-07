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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.runtime.spec.{Edition, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{longValue, stringValue}

import scala.collection.mutable.ArrayBuffer

abstract class ReactiveResultTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT])
  extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should handle requesting one record at a time") {
    //Given
    val subscriber = new TestSubscriber
    val result = runtimeResult(subscriber,
                            Array("1", 1),
                            Array("2", 2),
                            Array("3", 3))


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
    val result = runtimeResult(subscriber,
                               Array(1),
                               Array(2),
                               Array(3))

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
    val result = runtimeResult(subscriber,
                               Array(1),
                               Array(2),
                               Array(3))

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
    val result = runtimeResult(subscriber,
                               Array(1),
                               Array(2),
                               Array(3))

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
    val result = runtimeResult(subscriber,
                               Array(1),
                               Array(2),
                               Array(3))

    //When
    result.request(1)
    result.await() shouldBe true
    subscriber.lastSeen should equal(Array(longValue(1)))
    subscriber.isCompleted shouldBe false
    result.cancel()
    result.request(1)

    //Then
    result.await() shouldBe false
    subscriber.isCompleted shouldBe false
  }

  private def runtimeResult(subscriber: QuerySubscriber, data: Array[Any]*): RuntimeResult = {
    val variables = (1 to data.head.length).map(i => s"v$i")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(variables: _*)
      .input(variables = variables)
      .build()

    execute(logicalQuery, runtime, inputValues(data: _*), subscriber)
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

    override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
      done = true
    }

    def isCompleted: Boolean = done

    def lastSeen: Seq[AnyValue] = current

    //convert to list since nested array equality doesn't work nicely in tests
    def allSeen: Seq[Seq[AnyValue]] = records
  }

}
