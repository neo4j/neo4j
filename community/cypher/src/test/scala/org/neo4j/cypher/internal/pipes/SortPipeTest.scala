/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import collection.mutable.{Map=>MutableMap}
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.neo4j.cypher.internal.symbols.{NumberType, StringType, ScalarType}
import org.neo4j.cypher.internal.commands.SortItem

class SortPipeTest extends JUnitSuite {
  @Test def emptyInIsEmptyOut() {
    val source = new FakePipe(List(), "x" -> ScalarType())
    val sortPipe = new SortPipe(source, List(SortItem(Identifier("x"), true)))

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def simpleSortingIsSupported() {
    val list:Seq[MutableMap[String, Any]] = List(MutableMap("x" -> "B"), MutableMap("x" -> "A"))
    val source = new FakePipe(list, "x" -> StringType())
    val sortPipe = new SortPipe(source, List(SortItem(Identifier("x"), true)))

    assertEquals(List(MutableMap("x" -> "A"), MutableMap("x" -> "B")), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def sortByTwoColumns() {
    val source = new FakePipe(List(
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10)), "x" -> StringType(), "y"->NumberType())

    val sortPipe = new SortPipe(source, List(
      SortItem(Identifier("x"), true),
      SortItem(Identifier("y"), true)))

    assertEquals(List(
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10),
      MutableMap("x" -> "B", "y" -> 20)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def sortByTwoColumnsWithOneDescending() {
    val source = new FakePipe(List(
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10)), "x" -> StringType(), "y"->NumberType())

    val sortPipe = new SortPipe(source, List(
      SortItem(Identifier("x"), true),
      SortItem(Identifier("y"), false)))

    assertEquals(List(
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "B", "y" -> 10)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def shouldHandleSortingWithNullValues() {
    val list: Seq[MutableMap[String, Any]] = List(
      MutableMap("y" -> 1),
      MutableMap("y" -> null),
      MutableMap("y" -> 2))
    val source = new FakePipe(list, "y"->NumberType())

    val sortPipe = new SortPipe(source, List(SortItem(Identifier("y"), true)))

    assertEquals(List(
      MutableMap("y" -> 1),
      MutableMap("y" -> 2),
      MutableMap("y" -> null)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }
}
