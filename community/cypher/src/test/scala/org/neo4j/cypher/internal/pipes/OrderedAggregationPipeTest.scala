/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.junit.matchers.JUnitMatchers._
import scala.collection.JavaConverters._
import org.neo4j.cypher.internal.commands._
import org.scalatest.junit.JUnitSuite
import org.scalatest.Assertions
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.symbols._
import collection.mutable.Map

class OrderedAggregationPipeTest extends JUnitSuite with Assertions {
  @Test def shouldReturnColumnsFromReturnItems() {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = List(Entity("name"))
    val grouping = List(CountStar())
    val aggregationPipe = new OrderedAggregationPipe(source, returnItems, grouping)

    assertEquals(
      Seq(Identifier("name", NodeType()), Identifier("count(*)", LongType())),
      aggregationPipe.symbols.identifiers)
  }

  @Test def shouldThrowSemanticException() {
    val source = new FakePipe(List(), createSymbolTableFor("extractReturnItems"))

    val returnItems = List(Entity("name"))
    val grouping = List(Count(Entity("none-existing-identifier")))
    intercept[SyntaxException](new OrderedAggregationPipe(source, returnItems, grouping))
  }

  @Test def shouldAggregateCountStar() {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31),
      Map("name" -> "Peter", "age" -> 38)
    ), createSymbolTableFor("name"))

    val returnItems = List(Entity("name"))
    val grouping = List(CountStar())
    val aggregationPipe = new OrderedAggregationPipe(source, returnItems, grouping)

    assertThat(aggregationPipe.createResults(Map()).toIterable.asJava, hasItems(
      Map("name" -> "Andres", "count(*)" -> 1),
      Map("name" -> "Michael", "count(*)" -> 2),
      Map("name" -> "Peter", "count(*)" -> 1)))
  }

  @Test def shouldCountNonNullValues() {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> "Michael", "age" -> null),
      Map("name" -> "Peter", "age" -> 38)), createSymbolTableFor("name", "age"))

    val returnItems = List(Entity("name"))
    val grouping = List(Count(Entity("age")))
    val aggregationPipe = new OrderedAggregationPipe(source, returnItems, grouping)

    assertThat(aggregationPipe.createResults(Map()).toIterable.asJava, hasItems(
      Map("name" -> "Andres", "count(age)" -> 1),
      Map("name" -> "Michael", "count(age)" -> 0),
      Map("name" -> "Peter", "count(age)" -> 1)))
  }

  @Test def shouldThrowOnEmptyKeyList() {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = List()
    val grouping = List(Count(Entity("name")))
    intercept[ThisShouldNotHappenError](new OrderedAggregationPipe(source, returnItems, grouping))
  }

  private def createSymbolTableFor(names: String*) = new SymbolTable(names.map(Identifier(_, NodeType())): _*)

}
