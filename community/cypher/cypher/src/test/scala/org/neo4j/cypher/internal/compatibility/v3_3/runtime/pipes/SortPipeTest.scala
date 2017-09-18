/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.junit.Assert._
import org.neo4j.cypher.ValueComparisonHelper._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.intValue
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable.{Map => MutableMap}

class SortPipeTest extends CypherFunSuite with MockitoSugar {

  test("empty input gives empty output") {
    val source = new FakePipe(List(), "x" -> CTAny)
    val sortPipe = new SortPipe(source, List(Ascending("x")))()

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList)
  }

  test("simple sorting is supported") {
    val list: Seq[MutableMap[String, Any]] = List(MutableMap("x" -> "B"), MutableMap("x" -> "A"))
    val source = new FakePipe(list, "x" -> CTString)
    val sortPipe = new SortPipe(source, List(Ascending("x")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(Map("x" -> "A"), Map("x" -> "B")))
  }

  test("sort by two columns") {
    val source = new FakePipe(List(
      MutableMap[String, Any]("x" -> "B", "y" -> 20),
      MutableMap[String, Any]("x" -> "A", "y" -> 100),
      MutableMap[String, Any]("x" -> "B", "y" -> 10)), "x" -> CTString, "y" -> CTNumber)

    val sortPipe = new SortPipe(source, List(
      Ascending("x"),
      Ascending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map("x" -> "A", "y" -> 100),
      Map("x" -> "B", "y" -> 10),
      Map("x" -> "B", "y" -> 20)))
  }

  test("sort by two columns with one descending") {
    val source = new FakePipe(List(
      MutableMap[String, Any]("x" -> "B", "y" -> 20),
      MutableMap[String, Any]("x" -> "A", "y" -> 100),
      MutableMap[String, Any]("x" -> "B", "y" -> 10)), "x" -> CTString, "y" -> CTNumber)

    val sortPipe = new SortPipe(source, List(
      Ascending("x"),
      Descending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should beEquivalentTo(List(
      Map[String, Any]("x" -> "A", "y" -> 100),
      Map[String, Any]("x" -> "B", "y" -> 20),
      Map[String, Any]("x" -> "B", "y" -> 10)))
  }

  test("should handle null values") {
    val list: Seq[MutableMap[String, Any]] = List(
      MutableMap("y" -> 1),
      MutableMap("y" -> null),
      MutableMap("y" -> 2))
    val source = new FakePipe(list, "y" -> CTNumber)

    val sortPipe = new SortPipe(source, List(Ascending("y")))()

    sortPipe.createResults(QueryStateHelper.emptyWithValueSerialization).toList should equal(List(
      Map("y" -> intValue(1)),
      Map("y" -> intValue(2)),
      Map("y" -> Values.NO_VALUE)))
  }
}
