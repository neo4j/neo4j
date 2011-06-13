/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb._
import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.EntityOutput

class SortPipeTest {
  @Test def emptyInIsEmptyOut() {
    val inner = new StartPipe[Node]("x", List())
    val sortPipe = new SortPipe(List(SortItem(EntityOutput("x"), true)), inner)

    assertEquals(List(), sortPipe.toList)
  }

  @Test def simpleSortingIsSupported() {
    val inner = new FakePipe(List(Map("x" -> "B"), Map("x" -> "A")))
    val sortPipe = new SortPipe(List(SortItem(EntityOutput("x"), true)), inner)

    assertEquals(List(Map("x" -> "A"), Map("x" -> "B")), sortPipe.toList)
  }

  @Test def sortByTwoColumns() {
    val inner = new FakePipe(List(
      Map("x" -> "B", "y" -> 20),
      Map("x" -> "A", "y" -> 100),
      Map("x" -> "B", "y" -> 10)))

    val sortPipe = new SortPipe(List(
      SortItem(EntityOutput("x"), true),
      SortItem(EntityOutput("y"), true)), inner)

    assertEquals(List(
      Map("x" -> "A", "y" -> 100),
      Map("x" -> "B", "y" -> 10),
      Map("x" -> "B", "y" -> 20)), sortPipe.toList)
  }

  @Test def sortByTwoColumnsWithOneDescending() {
    val inner = new FakePipe(List(
      Map("x" -> "B", "y" -> 20),
      Map("x" -> "A", "y" -> 100),
      Map("x" -> "B", "y" -> 10)))

    val sortPipe = new SortPipe(List(
      SortItem(EntityOutput("x"), true),
      SortItem(EntityOutput("y"), false)), inner)

    assertEquals(List(
      Map("x" -> "A", "y" -> 100),
      Map("x" -> "B", "y" -> 20),
      Map("x" -> "B", "y" -> 10)), sortPipe.toList)
  }

}

class FakePipe(data: Seq[Map[String, Any]]) extends Pipe {
  val symbols: SymbolTable = new SymbolTable()

  def foreach[U](f: (Map[String, Any]) => U) {
    data.foreach(f(_))
  }
}