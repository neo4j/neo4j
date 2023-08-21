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

import org.neo4j.cypher.internal.runtime.interpreted.Ascending
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable

class PartialSortPipeTest extends CypherFunSuite {

  test("partial sort is lazy") {
    val list = List(
      mutable.Map("x" -> 3, "y" -> 1),
      mutable.Map("x" -> 3, "y" -> 2),
      mutable.Map("x" -> 5, "y" -> 9),
      mutable.Map("x" -> 5, "y" -> 9),
      mutable.Map("x" -> 5, "y" -> 0),
      mutable.Map("x" -> 5, "y" -> 7),
      mutable.Map("x" -> 6, "y" -> 1),
      mutable.Map("x" -> 6, "y" -> 1)
    )
    val source = new FakePipe(list)

    val sortPipe = PartialSortPipe(
      source,
      InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))),
      InterpretedExecutionContextOrdering.asComparator(List(Ascending("y"))),
      None
    )()

    val iterator = sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization)

    iterator.next() // 3, 1
    source.numberOfPulledRows should be(3)

    iterator.next() // 3, 2
    source.numberOfPulledRows should be(3)

    iterator.next() // 5, 0
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 7
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 9
    source.numberOfPulledRows should be(7)

    iterator.next() // 5, 9
    source.numberOfPulledRows should be(7)

    iterator.next() // 6, 1
    source.numberOfPulledRows should be(8)

    iterator.next() // 6, 1
    source.numberOfPulledRows should be(8)

    iterator.hasNext should be(false)
  }

}
