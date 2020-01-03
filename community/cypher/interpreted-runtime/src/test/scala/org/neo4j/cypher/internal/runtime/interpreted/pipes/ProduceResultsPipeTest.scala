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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscriberAdapter}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.{FALSE, TRUE, intValue, stringValue}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ProduceResultsPipeTest extends CypherFunSuite {

  test("should project needed columns") {
    val sourcePipe = mock[Pipe]
    val columns = Array("a", "b", "c")

    val records = ArrayBuffer.empty[Map[String, AnyValue]]

    val subscriber: QuerySubscriber = new QuerySubscriberAdapter {
      private val record: mutable.Map[String, AnyValue] = mutable.Map.empty
      private var currentOffset = -1

      override def onField(value: AnyValue): Unit = {
        try {
          record.put(columns(currentOffset), value)
        } finally {
          currentOffset += 1
        }
      }

      override def onRecordCompleted(): Unit = {
        currentOffset = -1
        records.append(record.toMap)
      }

      override def onRecord(): Unit = {
        currentOffset = 0
      }
    }
    val queryState = mock[QueryState]
    when(queryState.subscriber).thenReturn(subscriber)

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(
      Iterator(
        ExecutionContext.from("a" -> stringValue("foo"), "b" -> intValue(10), "c" -> TRUE, "d" -> stringValue("d")),
        ExecutionContext.from("a" -> stringValue("bar"), "b" -> intValue(20), "c" -> FALSE, "d" -> stringValue("d"))
      ))

    val pipe = ProduceResultsPipe(sourcePipe, columns)()

    pipe.createResults(queryState).toList

    records.toList should equal(
      List(
        Map("a" -> stringValue("foo"), "b" -> intValue(10), "c" -> TRUE),
        Map("a" -> stringValue("bar"), "b" -> intValue(20), "c" -> FALSE)
      ))
  }

  test("should produce no results if child pipe produces no results") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(Iterator.empty)

    val pipe = ProduceResultsPipe(sourcePipe, Array("a", "b", "c"))()

    val result = pipe.createResults(queryState).toList

    result shouldBe empty
  }
}
