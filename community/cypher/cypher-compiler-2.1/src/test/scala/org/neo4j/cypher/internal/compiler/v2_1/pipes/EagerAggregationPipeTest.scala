/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_1.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_1.symbols._

class EagerAggregationPipeTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("shouldReturnColumnsFromReturnItems") {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> CountStar())
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    aggregationPipe.symbols.identifiers should equal(Map("name" -> CTNode, "count(*)" -> CTInteger))
  }

  private def createReturnItemsFor(names: String*): Map[String, Identifier] = names.map(x => x -> Identifier(x)).toMap

  test("shouldThrowSemanticException") {
    val source = new FakePipe(List(), createSymbolTableFor("extractReturnItems"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> Count(Identifier("none-existing-identifier")))
    intercept[SyntaxException](new EagerAggregationPipe(source, returnItems, grouping))
  }

  test("shouldAggregateCountStar") {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> "Peter", "age" -> 38),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> CountStar())
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    getResults(aggregationPipe) should contain allOf(
      Map("name" -> "Andres", "count(*)" -> 1),
      Map("name" -> "Peter", "count(*)" -> 1),
      Map("name" -> "Michael", "count(*)" -> 2)
      )
  }

  test("shouldReturnZeroForEmptyInput") {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map(
      "count(*)" -> CountStar(),
      "avg(name.age)" -> Avg(Property(Identifier("name"), PropertyKey("age"))),
      "collect(name.age)" -> Collect(Property(Identifier("name"), PropertyKey("age"))),
      "count(name.age)" -> Count(Property(Identifier("name"), PropertyKey("age"))),
      "max(name.age)" -> Max(Property(Identifier("name"), PropertyKey("age"))),
      "min(name.age)" -> Min(Property(Identifier("name"), PropertyKey("age"))),
      "sum(name.age)" -> Sum(Property(Identifier("name"), PropertyKey("age")))
    )

    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    getResults(aggregationPipe) should contain(
      Map[String, Any]("avg(name.age)" -> null, "sum(name.age)" -> 0, "count(name.age)" -> 0, "min(name.age)" -> null, "collect(name.age)" -> List(), "max(name.age)" -> null, "count(*)" -> 0)
    )
  }

  test("shouldCountNonNullValues") {
    val source = new FakePipe(List(
      Map("name" -> "Andres", "age" -> 36),
      Map("name" -> null, "age" -> 38),
      Map("name" -> "Michael", "age" -> 36),
      Map("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map("count(name)" -> Count(Identifier("name")))
    val aggregationPipe = new EagerAggregationPipe(source, returnItems, grouping)

    getResults(aggregationPipe) should equal(List(Map("count(name)" -> 3)))
  }

  private def createSymbolTableFor(name: String) = name -> CTNode

  private def getResults(p: Pipe) = p.createResults(QueryStateHelper.empty).map(_.m.toMap).toList
}
