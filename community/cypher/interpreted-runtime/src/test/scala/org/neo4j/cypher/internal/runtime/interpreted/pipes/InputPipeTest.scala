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

import org.neo4j.cypher.internal.runtime.InputCursor
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.IteratorInputCursor
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

import scala.collection.mutable

class InputPipeTest extends CypherFunSuite {

  private class MyInputCursor(data: Iterator[Array[AnyValue]]) extends IteratorInputCursor(data) {
    private var _wasClosed = false

    override def close(): Unit = _wasClosed = true

    def wasClosed: Boolean = _wasClosed
  }

  private class MyInputStream(data: Array[Any]*) extends InputDataStream {
    private val cursors = mutable.ArrayBuffer[MyInputCursor]()

    override def nextInputBatch(): InputCursor = {
      val c = new MyInputCursor(data.map(_.map(ValueUtils.of)).toIterator)
      cursors += c
      c
    }
    def wasClosed: Boolean = cursors.forall(_.wasClosed)
  }

  test("close should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val inputStream = new MyInputStream(Array(10), Array(11), Array(12))
    val state = QueryStateHelper.emptyWith(
      query = QueryStateHelper.emptyWithResourceManager(resourceManager).query,
      input = inputStream
    )

    val pipe = InputPipe(Array("a"))()

    val result = pipe.createResults(state)
    result.close()
    inputStream.wasClosed shouldBe true
  }

  test("exhaust should close cursor") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val inputStream = new MyInputStream(Array(10), Array(11), Array(12))

    val pipe = InputPipe(Array("a"))()
    // exhaust
    pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).toList
    inputStream.wasClosed shouldBe true
  }
}
