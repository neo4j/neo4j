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
import org.junit.matchers.JUnitMatchers._
import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{NodeIdentifier, AggregationIdentifier, CountStar, EntityOutput}
import scala.collection.JavaConverters._


class AggregationPipeTest {
  @Test def shouldReturnColumnsFromReturnItems() {
    val source = new FakePipe(List(), new SymbolTable(Map("foo" -> NodeIdentifier("foo"))))

    val returnItems = List(EntityOutput("name"))
    val grouping = List(CountStar())
    val aggregationPipe = new AggregationPipe(source, returnItems, grouping)

    assertEquals(Map(
      "count(*)" -> AggregationIdentifier("count(*)"),
      "foo" -> NodeIdentifier("foo")), aggregationPipe.symbols.identifiers)
  }

  @Test def shouldAggregateCounts() {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> "Peter", "age" -> 38),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31)), new SymbolTable(Map("foo" -> NodeIdentifier("foo"))))

    val returnItems = List(EntityOutput("name"))
    val grouping = List(CountStar())
    val aggregationPipe = new AggregationPipe(source, returnItems, grouping)

    assertThat(aggregationPipe.toList.asJava, hasItems(
      Map("name" -> "Andres", "count(*)" -> 1),
      Map("name" -> "Peter", "count(*)" -> 1),
      Map("name" -> "Michael", "count(*)" -> 2)))
  }

}