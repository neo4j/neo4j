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

import org.junit.jupiter.api.Assertions.assertEquals
import org.neo4j.cypher.internal.runtime.interpreted.Ascending
import org.neo4j.cypher.internal.runtime.interpreted.Descending
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.intValue

import scala.collection.mutable

class SortPipeTest extends CypherFunSuite {

  test("empty input gives empty output") {
    val source = new FakePipe(List())
    val sortPipe = SortPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList)
  }

  test("simple sorting is supported") {
    val list: Seq[mutable.Map[String, Any]] = List(mutable.Map("x" -> "B"), mutable.Map("x" -> "A"))
    val source = new FakePipe(list)
    val sortPipe = SortPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> "A"),
      Map("x" -> "B")
    ))
  }

  test("sort by two columns") {
    val source = new FakePipe(List(
      mutable.Map[String, Any]("x" -> "B", "y" -> 20),
      mutable.Map[String, Any]("x" -> "A", "y" -> 100),
      mutable.Map[String, Any]("x" -> "B", "y" -> 10)
    ))

    val sortPipe =
      SortPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"), Ascending("y"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> "A", "y" -> 100),
      Map[String, Any]("x" -> "B", "y" -> 10),
      Map[String, Any]("x" -> "B", "y" -> 20)
    ))
  }

  test("sort by two columns with one descending") {
    val source = new FakePipe(List(
      mutable.Map[String, Any]("x" -> "B", "y" -> 20),
      mutable.Map[String, Any]("x" -> "A", "y" -> 100),
      mutable.Map[String, Any]("x" -> "B", "y" -> 10)
    ))

    val sortPipe =
      SortPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"), Descending("y"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> "A", "y" -> 100),
      Map[String, Any]("x" -> "B", "y" -> 20),
      Map[String, Any]("x" -> "B", "y" -> 10)
    ))
  }

  test("should handle null values") {
    val list: Seq[mutable.Map[String, Any]] = List(
      mutable.Map("y" -> 1),
      mutable.Map("y" -> null),
      mutable.Map("y" -> 2)
    )
    val source = new FakePipe(list)

    val sortPipe = SortPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("y"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("y" -> intValue(1)),
      Map("y" -> intValue(2)),
      Map("y" -> Values.NO_VALUE)
    ))
  }
}
