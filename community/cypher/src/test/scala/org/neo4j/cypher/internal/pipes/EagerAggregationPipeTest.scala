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
import org.junit.matchers.JUnitMatchers._
import scala.collection.JavaConverters._
import org.neo4j.cypher.internal.commands._
import expressions._
import org.scalatest.junit.JUnitSuite
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.symbols._
import collection.mutable.{Map => MutableMap}
import java.lang.{Iterable => JIterable}

class EagerAggregationPipeTest extends JUnitSuite {
  @Test def shouldReturnColumnsFromReturnItems() {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> CountStar())
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    assertEquals(
      Map("name" -> NodeType(), "count(*)" -> LongType()),
      aggregationPipe.symbols.identifiers)
  }


  private def createReturnItemsFor(names: String*): Map[String, Identifier] = names.map(x => x -> Identifier(x)).toMap

  @Test(expected = classOf[SyntaxException]) def shouldThrowSemanticException() {
    val source = new FakePipe(List(), createSymbolTableFor("extractReturnItems"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> Count(Identifier("none-existing-identifier")))
    new EagerAggregationPipe(source, returnItems, grouping)
  }

  @Test def shouldAggregateCountStar() {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> "Peter", "age" -> 38),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> CountStar())
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    assertThat(getResults(aggregationPipe), hasItems(
      Map("name" -> "Andres", "count(*)" -> 1),
      Map("name" -> "Peter", "count(*)" -> 1),
      Map("name" -> "Michael", "count(*)" -> 2)))
  }

  @Test def shouldReturnZeroForEmptyInput() {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map(
      "count(*)" -> CountStar(),
      "avg(name.age)" -> Avg(Property(Identifier("name"), "age")),
      "collect(name.age)" -> Collect(Property(Identifier("name"), "age")),
      "count(name.age)" -> Count(Property(Identifier("name"), "age")),
      "max(name.age)" -> Max(Property(Identifier("name"), "age")),
      "min(name.age)" -> Min(Property(Identifier("name"), "age")),
      "sum(name.age)" -> Sum(Property(Identifier("name"), "age"))
    )

    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    val results = getResults(aggregationPipe)
    assertThat(results, hasItems(Map[String, Any]("avg(name.age)" -> null, "sum(name.age)" -> 0, "count(name.age)" -> 0, "min(name.age)" -> null, "collect(name.age)" -> List(), "max(name.age)" -> null, "count(*)" -> 0)))
  }

  @Test def shouldCountNonNullValues() {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> null, "age" -> 38),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map("count(name)" -> Count(Identifier("name")))
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    assertEquals(List(Map("count(name)" -> 3)), aggregationPipe.createResults(QueryStateHelper.empty).toList)
  }

  private def createSymbolTableFor(name: String) = name -> NodeType()

  private def getResults(p: Pipe): JIterable[Map[String, Any]] = p.createResults(QueryStateHelper.empty).map(_.m.toMap).toIterable.asJava
}