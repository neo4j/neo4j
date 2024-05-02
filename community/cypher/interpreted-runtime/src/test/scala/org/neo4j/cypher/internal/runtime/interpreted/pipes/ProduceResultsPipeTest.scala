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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ReferenceByName
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.FALSE
import org.neo4j.values.storable.Values.TRUE
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.stringValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ProduceResultsPipeTest extends CypherFunSuite {

  test("should project needed columns") {
    val sourcePipe = mock[Pipe]
    val columns = Array("a", "b", "c")

    val records = ArrayBuffer.empty[Map[String, AnyValue]]

    val subscriber: QuerySubscriber = new QuerySubscriberAdapter {
      private val record: mutable.Map[String, AnyValue] = mutable.Map.empty

      override def onField(offset: Int, value: AnyValue): Unit = {
        record.put(columns(offset), value)
      }

      override def onRecordCompleted(): Unit = {
        records.append(record.toMap)
      }
    }
    val queryState = mock[QueryState]
    when(queryState.subscriber).thenReturn(subscriber)

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(
      ClosingIterator(Iterator(
        CypherRow.from("a" -> stringValue("foo"), "b" -> intValue(10), "c" -> TRUE, "d" -> stringValue("d")),
        CypherRow.from("a" -> stringValue("bar"), "b" -> intValue(20), "c" -> FALSE, "d" -> stringValue("d"))
      ))
    )

    val pipe = ProduceResultsPipe(sourcePipe, columns.map(ReferenceByName))()

    pipe.createResults(queryState).toList

    records.toList should equal(
      List(
        Map("a" -> stringValue("foo"), "b" -> intValue(10), "c" -> TRUE),
        Map("a" -> stringValue("bar"), "b" -> intValue(20), "c" -> FALSE)
      )
    )
  }

  test("should produce no results if child pipe produces no results") {
    val sourcePipe = mock[Pipe]
    val queryState = mock[QueryState]

    when(queryState.decorator).thenReturn(NullPipeDecorator)
    when(sourcePipe.createResults(queryState)).thenReturn(ClosingIterator.empty)

    val pipe = ProduceResultsPipe(sourcePipe, Array("a", "b", "c").map(ReferenceByName))()

    val result = pipe.createResults(queryState).toList

    result shouldBe empty
  }
}
