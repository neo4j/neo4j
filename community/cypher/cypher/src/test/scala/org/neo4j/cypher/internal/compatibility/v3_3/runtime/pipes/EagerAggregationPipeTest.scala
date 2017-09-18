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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{intValue, longValue, stringValue}
import org.neo4j.values.virtual.VirtualValues

class EagerAggregationPipeTest extends CypherFunSuite {

  private def createReturnItemsFor(names: String*): Map[String, Variable] = names.map(k => k -> Variable(k)).toMap

  test("should aggregate count(*) on single grouping column") {
    val source = new FakePipe(List(
      Map[String, Any]("name" -> "Andres", "age" -> 36),
      Map[String, Any]("name" -> "Peter", "age" -> 38),
      Map[String, Any]("name" -> "Michael", "age" -> 36),
      Map[String, Any]("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val grouping = createReturnItemsFor("name")
    val aggregation = Map("count(*)" -> CountStar())
    val aggregationPipe = EagerAggregationPipe(source, grouping, aggregation)()

    getResults(aggregationPipe) should contain allOf(
      Map[String, AnyValue]("name" -> stringValue("Andres"), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("name" -> stringValue("Peter"), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("name" -> stringValue("Michael"), "count(*)" -> longValue(2))
    )
  }

  test("should aggregate count(*) on two grouping columns") {
    def source = new FakePipe(List(
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 2),
      Map[String, Any]("a" -> 2, "b" -> 2)), createSymbolTableFor("a"), createSymbolTableFor("b"))

    val grouping = createReturnItemsFor("a", "b")
    val aggregation = Map("count(*)" -> CountStar())
    def aggregationPipe = EagerAggregationPipe(source, grouping, aggregation)()

    getResults(aggregationPipe) should contain allOf(
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(1), "count(*)" -> longValue(2)),
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(2), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("a" -> intValue(2), "b" -> intValue(2), "count(*)" -> longValue(1))
    )
  }

  test("should aggregate count(*) on three grouping columns") {
    def source = new FakePipe(List(
      Map[String, Any]("a" -> 1, "b" -> 1, "c" -> 1),
      Map[String, Any]("a" -> 1, "b" -> 1, "c" -> 2),
      Map[String, Any]("a" -> 1, "b" -> 2, "c" -> 3),
      Map[String, Any]("a" -> 2, "b" -> 2, "c" -> 4)),
      createSymbolTableFor("a"), createSymbolTableFor("b"), createSymbolTableFor("c"))

    val grouping = createReturnItemsFor("a", "b", "c")
    val aggregation = Map("count(*)" -> CountStar())
    def aggregationPipe = EagerAggregationPipe(source, grouping, aggregation)()

    getResults(aggregationPipe) should contain allOf(
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(1), "c" -> intValue(1), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(1), "c" -> intValue(2), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("a" -> intValue(1), "b" -> intValue(2), "c" -> intValue(3), "count(*)" -> longValue(1)),
      Map[String, AnyValue]("a" -> intValue(2), "b" -> intValue(2), "c" -> intValue(4), "count(*)" -> longValue(1))
    )
  }

  test("should handle grouping on null") {
    val source = new FakePipe(List(
      Map[String, Any]("name" -> "Apa"),
      Map[String, Any]("name" -> "Apa"),
      Map[String, Any]("name" -> null),
      Map[String, Any]("name" -> null)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor("name")
    val grouping = Map("count(*)" -> CountStar())
    val aggregationPipe = EagerAggregationPipe(source, returnItems, grouping)()

    getResults(aggregationPipe) should contain allOf(
      Map[String, AnyValue]("name" -> stringValue("Apa"), "count(*)" -> longValue(2)),
      Map[String, AnyValue]("name" -> Values.NO_VALUE, "count(*)" -> longValue(2))
    )
  }

  test("shouldReturnZeroForEmptyInput") {
    val source = new FakePipe(List(), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map(
      "count(*)" -> CountStar(),
      "avg(name.age)" -> Avg(Property(Variable("name"), PropertyKey("age"))),
      "collect(name.age)" -> Collect(Property(Variable("name"), PropertyKey("age"))),
      "count(name.age)" -> Count(Property(Variable("name"), PropertyKey("age"))),
      "max(name.age)" -> Max(Property(Variable("name"), PropertyKey("age"))),
      "min(name.age)" -> Min(Property(Variable("name"), PropertyKey("age"))),
      "sum(name.age)" -> Sum(Property(Variable("name"), PropertyKey("age")))
    )

    val aggregationPipe = EagerAggregationPipe(source, returnItems, grouping)()

    getResults(aggregationPipe) should contain(
      Map[String, AnyValue]("avg(name.age)" -> Values.NO_VALUE, "sum(name.age)" -> longValue(0),
                            "count(name.age)" -> longValue(0), "min(name.age)" -> Values.NO_VALUE,
                            "collect(name.age)" -> VirtualValues.EMPTY_LIST, "max(name.age)" -> Values.NO_VALUE,
                            "count(*)" -> longValue(0))
    )
  }

  test("shouldCountNonNullValues") {
    val source = new FakePipe(List(
      Map[String, Any]("name" -> "Andres", "age" -> 36),
      Map[String, Any]("name" -> null, "age" -> 38),
      Map[String, Any]("name" -> "Michael", "age" -> 36),
      Map[String, Any]("name" -> "Michael", "age" -> 31)), createSymbolTableFor("name"))

    val returnItems = createReturnItemsFor()
    val grouping = Map("count(name)" -> Count(Variable("name")))
    val aggregationPipe = EagerAggregationPipe(source, returnItems, grouping)()

    getResults(aggregationPipe) should equal(List(Map("count(name)" -> longValue(3))))
  }

  private def createSymbolTableFor(name: String): (String, CypherType) = name -> CTNode

  private def getResults(p: Pipe) = p.createResults(QueryStateHelper.empty).map(_.toMap).toList
}
