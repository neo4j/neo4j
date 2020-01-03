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

import java.util.Comparator

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.{Ascending, InterpretedExecutionContextOrdering, QueryStateHelper}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PartialTopNPipeTest extends CypherFunSuite {

  private val compareX: Comparator[ExecutionContext] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("x")))
  private val compareY: Comparator[ExecutionContext] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("y")))

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
    val sortPipe = PartialTopNPipe(source, Literal(5), compareX, compareY)()

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

  test("partial top -1 should be very lazy") {
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
    val sortPipe = PartialTopNPipe(source, Literal(-1), compareX, compareY)()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.hasNext should be(false)
    source.numberOfPulledRows should be(1)
  }
}
