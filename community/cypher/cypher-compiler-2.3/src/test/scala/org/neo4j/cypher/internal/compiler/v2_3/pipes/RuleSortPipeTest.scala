/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.commands.SortItem
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Add, Identifier, Literal, RandFunction}
import org.neo4j.cypher.internal.frontend.v2_3.PatternException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.collection.mutable.{Map => MutableMap}
import scala.util.Random

class RuleSortPipeTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("emptyInIsEmptyOut") {
    val source = new FakePipe(List(), "x" -> CTAny)
    val sortPipe = new LegacySortPipe(source, List(SortItem(Identifier("x"), ascending = true)))

    sortPipe.createResults(QueryStateHelper.empty).toList shouldBe empty
  }

  test("simpleSortingIsSupported") {
    val list:Seq[MutableMap[String, Any]] = List(MutableMap("x" -> "B"), MutableMap("x" -> "A"))
    val source = new FakePipe(list, "x" -> CTString)
    val sortPipe = new LegacySortPipe(source, List(SortItem(Identifier("x"), ascending = true)))

    sortPipe.createResults(QueryStateHelper.empty).toList should equal(List(
      MutableMap("x" -> "A"), MutableMap("x" -> "B")
    ))
  }

  test("sortByTwoColumns") {
    val source = new FakePipe(List(
      MutableMap[String, Any]("x" -> "B", "y" -> 20),
      MutableMap[String, Any]("x" -> "A", "y" -> 100),
      MutableMap[String, Any]("x" -> "B", "y" -> 10)), "x" -> CTString, "y" -> CTNumber)

    val sortPipe = new LegacySortPipe(source, List(
      SortItem(Identifier("x"), ascending = true),
      SortItem(Identifier("y"), ascending = true)))

    sortPipe.createResults(QueryStateHelper.empty).toList should equal(List(
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10),
      MutableMap("x" -> "B", "y" -> 20)
    ))
  }

  test("sortByTwoColumnsWithOneDescending") {
    val source = new FakePipe(List(
      MutableMap[String, Any]("x" -> "B", "y" -> 20),
      MutableMap[String, Any]("x" -> "A", "y" -> 100),
      MutableMap[String, Any]("x" -> "B", "y" -> 10)), "x" -> CTString, "y" -> CTNumber)

    val sortPipe = new LegacySortPipe(source, List(
      SortItem(Identifier("x"), ascending = true),
      SortItem(Identifier("y"), ascending = false)))

    sortPipe.createResults(QueryStateHelper.empty).toList should equal(List(
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "B", "y" -> 10)
    ))
  }

  test("shouldHandleSortingWithNullValues") {
    val list: Seq[MutableMap[String, Any]] = List(
      MutableMap("y" -> 1),
      MutableMap("y" -> null),
      MutableMap("y" -> 2))
    val source = new FakePipe(list, "y"->CTNumber)

    val sortPipe = new LegacySortPipe(source, List(SortItem(Identifier("y"), ascending = true)))

    sortPipe.createResults(QueryStateHelper.empty).toList should equal(List(
      MutableMap("y" -> 1),
      MutableMap("y" -> 2),
      MutableMap("y" -> null)
    ))
  }

  test("shouldHandleSortingWithComputedValues") {
    val list:Seq[MutableMap[String, Any]] = List(
      MutableMap("x" -> 3),
      MutableMap("x" -> 1),
      MutableMap("x" -> 2))

    val source = new FakePipe(list, "x" -> CTNumber)

    val sortPipe = new LegacySortPipe(source, List(SortItem(Add(Identifier("x"), Literal(1)), true)))

    val actualResult = sortPipe.createResults(QueryStateHelper.empty).toList
    val expectedResult =  List(
      MutableMap("x" -> 1),
      MutableMap("x" -> 2),
      MutableMap("x" -> 3))
    actualResult should equal(expectedResult)
  }

  test("shouldNotAllowSortingWithRandomValues") {
    val list:Seq[MutableMap[String, Any]] = Random.shuffle(
      for (v <- 1 to 1000) yield MutableMap("x" -> (v: Any)))

    val source = new FakePipe(list, "x" -> CTNumber)

    val sortPipe = new LegacySortPipe(source, List(SortItem(Add(Add(Literal(1), RandFunction()), Literal(1)), true)))

    intercept[PatternException](sortPipe.createResults(QueryStateHelper.empty))
  }
}
