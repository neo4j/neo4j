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
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.util.Comparator

class PartialTop1PipeTest extends CypherFunSuite {

  private val compareX: Comparator[ReadableRow] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("x")))
  private val compareY: Comparator[ReadableRow] = InterpretedExecutionContextOrdering.asComparator(List(Ascending("y")))

  test("partial top 1 should be lazy") {
    val input = List(
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 5),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 2, "y" -> 4),
      Map("x" -> 2, "y" -> 3),
      Map("x" -> 2, "y" -> 4)
    )

    val source = new FakePipe(input)
    val sortPipe = PartialTop1Pipe(source, compareX, compareY)()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.next() // first 1, 2
    source.numberOfPulledRows should be(5)

    iterator.hasNext should be(false)
  }
}
