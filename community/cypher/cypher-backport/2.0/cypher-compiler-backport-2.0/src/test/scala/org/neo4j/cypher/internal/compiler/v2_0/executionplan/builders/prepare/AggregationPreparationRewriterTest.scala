/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.prepare

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedProperty
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.BuilderTest
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.LiteralMap
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Divide
import org.neo4j.cypher.internal.compiler.v2_0.commands.AllIdentifiers
import org.neo4j.cypher.internal.compiler.v2_0.commands.SortItem
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Add
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Collect
import org.neo4j.cypher.internal.compiler.v2_0.commands.SingleNode
import org.neo4j.cypher.internal.compiler.v2_0.commands.Equals
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.CountStar
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property

class AggregationPreparationRewriterTest extends BuilderTest {

  def builder: PlanBuilder = AggregationPreparationRewriter(Some(namer))

  @Test
  def should_just_pass_through_queries_without_aggregation() {
    // given MATCH n RETURN n
    val q = Query.
      matches(SingleNode("n")).
      returns(ReturnItem(Identifier("n"), "n"))

    // when
    assertRejects(q)
  }

  @Test
  def should_split_query_up_into_two() {
    // given MATCH n RETURN n.foo + count(*)
    val q = Query.
      matches(n).
      returns(ReturnItem(Add(nFoo, countStar), addNFooCountStarString))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n.foo AS x1, count(*) AS x2 RETURN x1 + x2
    val expectedTail =
      Query.
        start().
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStarString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
        ReturnItem(countStar, countStarString, renamed = true),
        ReturnItem(nFoo, nFooString, renamed = true)
    )
    val expectedQuery = PartiallySolvedQuery(expected)

    assert(result === expectedQuery)
  }

  @Test
  def should_split_complex_query_up_into_two() {
    // given MATCH n RETURN count(*)/60/60
    val q = Query.
      matches(n).
      returns(ReturnItem(countStarDiv60Div60, countStarDiv60Div60String))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH count(*)/60 AS x1, 60 AS x2 RETURN x1/x2 AS count(*)/60/60
    val expectedTail =
      Query.
        start().
        returns(ReturnItem(Divide(Identifier(countStarDiv60String), Identifier(literal60String)), countStarDiv60Div60String))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
        ReturnItem(countStarDiv60, countStarDiv60String, renamed = true),
        ReturnItem(literal60, literal60String, renamed = true)
      )

    val expectedQuery = PartiallySolvedQuery(expected)

    assert(result === expectedQuery)
  }

  @Test
  def should_split_inv_complex_query_up_into_two() {
    // given MATCH n RETURN 60/60/count(*)
    val q = Query.
      matches(n).
      returns(ReturnItem(div60by60byCountStar, div60by60byCountStarString))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH 60 AS x1, 60/count(*) AS x2 RETURN x1/x2 AS 60/60/count(*)
    val expectedTail =
      Query.
        start().
        returns(ReturnItem(Divide(Identifier(literal60String), Identifier(div60byCountStarString)), div60by60byCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
        ReturnItem(div60byCountStar, div60byCountStarString, renamed = true),
        ReturnItem(literal60, literal60String, renamed = true)
      )

    val expectedQuery = PartiallySolvedQuery(expected)

    assert(result === expectedQuery)
  }

  @Test
  def should_handle_star() {
    // given MATCH n RETURN *, n.foo + count(*)
    val q = Query.
      matches(n).
      returns(AllIdentifiers(), ReturnItem(Add(nFoo, countStar), addNFooCountStarString))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH *, n.foo AS x1, count(*) AS x2 RETURN *, x1 + x2
    val expectedTail =
      Query.
        start().
        returns(AllIdentifiers(), ReturnItem(Add(Identifier(nFooString), Identifier(countStarString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(countStar, countStarString, renamed = true),
      ReturnItem(nFoo, nFooString, renamed = true),
      AllIdentifiers()
    )

    val expectedPsq = PartiallySolvedQuery(expected)
    assert(result === expectedPsq)
  }

  @Test
  def should_split_query_up_into_two_and_remember_order_by() {
    // given MATCH n RETURN n.foo + count(*)
    val q = Query.
      matches(n).
      orderBy(SortItem(nFoo, true)).
      returns(ReturnItem(Add(nFoo, countStar), addNFooCountStarString))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n.foo AS x1, count(*) AS x2 RETURN x1 + x2
    val expectedTail =
      Query.
        start().
        orderBy(SortItem(Identifier(nFooString), true)).
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStarString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(countStar, countStarString, renamed = true),
      ReturnItem(nFoo, nFooString, renamed = true)
    )

    assert(result === PartiallySolvedQuery(expected))
  }

  @Test
  def should_keep_other_expressions() {
    // given MATCH n RETURN n.foo + count(*), n.bar
    val q = Query.
      matches(n).
      returns(
      ReturnItem(Add(nFoo, countStar), addNFooCountStarString),
      ReturnItem(nBar, nBarString)
    )

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n.bar as x1, n.foo AS x2, count(*) AS x3 RETURN x1, x2 + x3
    val expectedTail =
      Query.
        start().
        returns(
        ReturnItem(Add(Identifier(nFooString), Identifier(countStarString)), addNFooCountStarString),
        ReturnItem(Identifier(nBarString), nBarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
        ReturnItem(nBar, nBarString, renamed = true),
        ReturnItem(countStar, countStarString, renamed = true),
        ReturnItem(nFoo, nFooString, renamed = true)
      )

    val query = PartiallySolvedQuery(expected)
    assert(result === query)
  }

  @Test
  def should_handle_literal_map_with_keys_and_aggregates_as_values() {
    // given  MATCH (a:Start)<-[:R]-(b) RETURN { foo:a.prop=42, bar:collect(b.prop2) }
    val q = Query.
      matches(r).
      returns(
        ReturnItem(LiteralMap(Map("foo" -> aProp, "bar" -> collect)), "literal-map")
      )

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH a.pro=42 as x1, collect(b.prop2) AS x2 RETURN { foo: x1, bar: x2 }
    val expectedTail =
      Query.
        start().
        returns(
          ReturnItem(LiteralMap(Map("foo" -> Identifier(aPropString), "bar" -> Identifier(collectString))), "literal-map"))

    val expected = Query.
      matches(r).
      tail(expectedTail).
      returns(
        ReturnItem(collect, collectString, renamed = true),
        ReturnItem(aProp, aPropString, renamed = true)
      )

    val expectation = PartiallySolvedQuery(expected)
    assert(result === expectation)
  }

  @Test
  def should_not_go_into_a_never_ending_loop() {
    // given MATCH n RETURN n, count(n) + 3
    val q = Query.
      matches(n).
      returns(
        ReturnItem(nIdent, nIdentString),
        ReturnItem(countNAdd3, countNAdd3String)
      )

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n as x1, 3 as x3, count(n) AS x2 RETURN x1, x2 + x3
    val expectedTail =
      Query.
        start().
        returns(
          ReturnItem(nIdent, nIdentString),
          ReturnItem(Add(Identifier(countNString), Identifier(threeString)), countNAdd3String))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
        ReturnItem(three, threeString, renamed = true),
        ReturnItem(countN, countNString, renamed = true),
        ReturnItem(nIdent, nIdentString, renamed = true))

    val query = PartiallySolvedQuery(expected)
    assert(result === query)
  }

  val nIdent = Identifier("n")
  val nFoo = Property(nIdent, UnresolvedProperty("foo"))
  val nBar = Property(nIdent, UnresolvedProperty("bar"))
  val n = SingleNode("n")
  val r = RelatedTo("a", "b", "r", "R", Direction.OUTGOING)
  val aProp = Equals(Property(Identifier("a"), UnresolvedProperty("prop")), Literal(42))
  val collect = Collect(Property(Identifier("b"), UnresolvedProperty("prop2")))
  val countN = Count(nIdent)
  val three = Literal(3)
  val countNAdd3 = Add(countN, three)

  val countStar = CountStar()
  val literal60 = Literal(60)
  val countStarDiv60 =  Divide(countStar, literal60)
  val countStarDiv60Div60 = Divide(countStarDiv60, literal60)
  val div60byCountStar = Divide(literal60, countStar)
  val div60by60byCountStar = Divide(literal60, div60byCountStar)

  val countNString = "count(n)"
  val threeString = "3"
  val nFooString = "n.foo"
  val nBarString = "n.bar"
  val nIdentString = "n"
  val countStartString = "count(*)"
  val addNFooCountStarString = "n.foo + count(*)"
  val countNAdd3String = "count(n) + 3"
  val aPropString = "a.prop=42"
  val collectString = "collect(b.prop2)"
  val countStarString = "count(*)"
  val countStarDiv60String = "count(*)/60"
  val countStarDiv60Div60String = "count(*)/60/60"
  val literal60String = "60"
  val div60byCountStarString = "60/count(*)"
  val div60by60byCountStarString = "60/60/count(*)"


  val namer = Map[Expression, String](
    countStar -> countStarString,
    nFoo -> nFooString,
    nBar -> nBarString,
    aProp -> aPropString,
    literal60 -> literal60String,
    countStarDiv60 -> countStarDiv60String,
    countStarDiv60Div60 -> countStarDiv60Div60String,
    div60byCountStar -> div60byCountStarString,
    div60by60byCountStar -> div60by60byCountStarString,
    collect -> collectString,
    countN -> countNString,
    countNAdd3 -> countNAdd3String,
    three -> threeString,
    nIdent -> nIdentString
  )
}
