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

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.Ascending
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.IntValue

import java.util.Comparator

class PartialTopNPipeTest extends CypherFunSuite {

  private val compareX: Comparator[ReadableRow] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("x")))
  private val compareY: Comparator[ReadableRow] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("y")))

  test("partial top n should be lazy") {
    val input = List(
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 2, "y" -> 4),
      Map("x" -> 2, "y" -> 3),
      Map("x" -> 2, "y" -> 5),
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 0),
      Map("x" -> 3, "y" -> 5),
      Map("x" -> 3, "y" -> 7),
      Map("x" -> 3, "y" -> 1)
    )

    val source = new FakePipe(input)
    val sortPipe = PartialTopNPipe(source, literal(5), None, compareX, compareY)()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.next() // first 1, 2
    source.numberOfPulledRows should be(5)
    iterator.next() // second 1, 2
    source.numberOfPulledRows should be(5)
    iterator.next() // first 1, 5
    source.numberOfPulledRows should be(5)
    iterator.next() // second 1, 5
    source.numberOfPulledRows should be(5)
    iterator.next() // 2, 3
    source.numberOfPulledRows should be(8)

    iterator.hasNext should be(false)
  }

  test("partial top if LIMIT aligns with chunk boundary") {
    val input = List(
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 2, "y" -> 3),
      Map("x" -> 2, "y" -> 5),
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 0),
      Map("x" -> 3, "y" -> 5)
    )

    val source = new FakePipe(input)
    val sortPipe = PartialTopNPipe(source, literal(4), None, compareX, compareY)()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.toList.map(r =>
      Map(
        "x" -> r.getByName("x").asInstanceOf[IntValue].longValue(),
        "y" -> r.getByName("y").asInstanceOf[IntValue].longValue()
      )
    ) shouldBe
      input.sortBy(m => (m("x"), m("y"))).take(4)
  }

  test("partial top 0 should be very lazy") {
    val input = List(
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 2, "y" -> 4),
      Map("x" -> 2, "y" -> 3),
      Map("x" -> 2, "y" -> 5),
      Map("x" -> 3, "y" -> 1),
      Map("x" -> 3, "y" -> 0),
      Map("x" -> 3, "y" -> 5),
      Map("x" -> 3, "y" -> 7),
      Map("x" -> 3, "y" -> 1)
    )

    val source = new FakePipe(input)
    val sortPipe = PartialTopNPipe(source, literal(0), None, compareX, compareY)()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.hasNext should be(false)
    source.numberOfPulledRows should be(0)
  }
}
