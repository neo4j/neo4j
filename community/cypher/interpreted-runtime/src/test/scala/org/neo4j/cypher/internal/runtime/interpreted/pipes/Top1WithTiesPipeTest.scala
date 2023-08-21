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
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class Top1WithTiesPipeTest extends CypherFunSuite {

  test("empty input gives empty output") {
    val source = new FakePipe(List())
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization) should be(empty)
  }

  test("simple sorting works as expected") {
    val list = List(Map("x" -> "B"), Map("x" -> "A")).iterator
    val source = new FakePipe(list)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(
      List(Map("x" -> "A"))
    )
  }

  test("two ties for the first place are all returned") {
    val input = List(
      Map("x" -> 1, "y" -> 1),
      Map("x" -> 1, "y" -> 2),
      Map("x" -> 2, "y" -> 3),
      Map("x" -> 2, "y" -> 4)
    ).iterator

    val source = new FakePipe(input)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 1, "y" -> 1),
      Map("x" -> 1, "y" -> 2)
    ))
  }

  test("if only null is present, it should be returned") {
    val input = List(
      Map[String, Any]("x" -> null, "y" -> 1),
      Map[String, Any]("x" -> null, "y" -> 2)
    ).iterator

    val source = new FakePipe(input)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> null, "y" -> 1),
      Map[String, Any]("x" -> null, "y" -> 2)
    ))
  }

  test("null should not be returned if other values are present") {
    val input = List(
      Map[String, Any]("x" -> 1, "y" -> 1),
      Map[String, Any]("x" -> null, "y" -> 2),
      Map[String, Any]("x" -> 2, "y" -> 3)
    ).iterator

    val source = new FakePipe(input)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> 1, "y" -> 1)
    ))
  }

  test("comparing arrays") {
    val smaller = Array(1, 2)
    val input = List(
      Map[String, Any]("x" -> Array(3, 4), "y" -> 2),
      Map[String, Any]("x" -> smaller, "y" -> 1)
    ).iterator

    val source = new FakePipe(input)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> smaller, "y" -> 1)
    ))
  }

  test("comparing numbers and strings") {
    val input = List(
      Map[String, Any]("x" -> 1, "y" -> 1),
      Map[String, Any]("x" -> "A", "y" -> 2)
    ).iterator

    val source = new FakePipe(input)
    val sortPipe = Top1WithTiesPipe(source, InterpretedExecutionContextOrdering.asComparator(List(Ascending("x"))))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> "A", "y" -> 2)
    ))
  }

}
