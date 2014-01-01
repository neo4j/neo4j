/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_0.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Add
import org.neo4j.cypher.internal.compiler.v2_0.commands.SingleNode
import org.neo4j.cypher.internal.compiler.v2_0.commands.AllIdentifiers
import org.neo4j.cypher.internal.compiler.v2_0.commands.SortItem
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.CountStar
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property
import org.neo4j.graphdb.Direction

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
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true)
    )

    assert(result === PartiallySolvedQuery(expected))
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
        returns(AllIdentifiers(), ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true),
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
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true)
    )

    assert(result === PartiallySolvedQuery(expected))
  }

  @Test
  def should_keep_other_expressions() {
    // given MATCH n RETURN n.bar, n.foo + count(*)
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
        ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString),
        ReturnItem(Identifier(nBarString), nBarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true),
      ReturnItem(nBar, nBarString, renamed = true))

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
        ReturnItem(aProp, aPropString, renamed = true),
        ReturnItem(collect, collectString, renamed = true))

    val expectation = PartiallySolvedQuery(expected)
    assert(result === expectation)
  }

  val nFoo = Property(Identifier("n"), UnresolvedProperty("foo"))
  val nBar = Property(Identifier("n"), UnresolvedProperty("bar"))
  val n = SingleNode("n")
  val r = RelatedTo("a", "b", "r", "R", Direction.OUTGOING)
  val aProp = Equals(Property(Identifier("a"), UnresolvedProperty("prop")), Literal(42))
  val collect = Collect(Property(Identifier("b"), UnresolvedProperty("prop2")))

  val nFooString = "n.foo"
  val nBarString = "n.bar"
  val countStartString = "count(*)"
  val addNFooCountStarString = "n.foo + count(*)"
  val aPropString = "a.prop=42"
  val collectString = "collect(b.prop2)"
  val countStar = CountStar()
  val namer = Map[Expression, String](
    countStar -> countStartString,
    nFoo -> nFooString,
    nBar -> nBarString,
    aProp -> aPropString,
    collect -> collectString)
}
