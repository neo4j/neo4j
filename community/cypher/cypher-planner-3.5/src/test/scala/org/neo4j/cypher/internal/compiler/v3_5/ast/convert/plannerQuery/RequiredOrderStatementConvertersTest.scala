/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.AscSortItem
import org.neo4j.cypher.internal.v3_5.ast.DescSortItem
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class RequiredOrderStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Extracts required order from query returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from distinct") {
    val result = buildPlannerQuery("MATCH (n) RETURN DISTINCT n.prop ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = DistinctQueryProjection(Map("n.prop" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from aggregation") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop, count(*) ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = AggregatingQueryProjection(Map("n.prop" -> prop("n", "prop")), Map("count(*)" -> CountStar()(pos)), QueryShuffle(Seq(AscSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop2 ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("n.prop2" -> prop("n", "prop2")), QueryShuffle(Seq(AscSortItem(prop("n", "prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order if variable is not projected") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop2 DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.desc("n.prop2"),
      horizon = RegularQueryProjection(projections = Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)),
        shuffle = QueryShuffle(List(DescSortItem(Property(Variable("n")(pos), PropertyKeyName("prop2")(pos))(pos))(pos)), None, None))
    )

    result should equal(expectation)
  }

  test("Extracts descending required order from query returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.desc("n.prop"),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")), QueryShuffle(Seq(DescSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column, but a dependency") {
    val result = buildPlannerQuery("MATCH (n) RETURN n ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("n" -> varFor("n")), QueryShuffle(Seq(AscSortItem(prop("n", "prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Propagate interesting order to previous query graph") {
    val result = buildPlannerQuery("MATCH (n) WITH n AS secretN MATCH (m) RETURN m, secretN ORDER BY secretN.prop")

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.ascInteresting("n.prop"),
      InterestingOrder.asc("secretN.prop")
    ))
  }

  test("Do not propagate unfulfillable order to previous query graph") {
    val result = buildPlannerQuery("MATCH (n) WITH n AS secretN MATCH (m) RETURN m ORDER BY m.prop")

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.empty,
      InterestingOrder.asc("m.prop")
    ))
  }

  test("Do not propagate interesting order over required order") {
    val result = buildPlannerQuery(
      """MATCH (a) WITH a AS a2
        |MATCH (b) WITH b AS b2, a2 ORDER BY a2.prop
        |MATCH (c) WITH c AS c2, b2, a2
        |MATCH (d) RETURN d, c2, b2, a2 ORDER BY c2.prop""".stripMargin)

    interestingOrders(result).take(4) should be(List(
      InterestingOrder.ascInteresting("a.prop"),
      InterestingOrder.asc("a2.prop"),
      InterestingOrder.ascInteresting("c.prop"),
      InterestingOrder.asc("c2.prop")
    ))
  }

  ignore("Propagate suffix of interesting order if the interesting prefix overlaps the required order") {
    val result = buildPlannerQuery(
      """MATCH (a) WITH a AS a2
        |MATCH (b) WITH b AS b2, a2 ORDER BY a2.prop
        |MATCH (c) WITH c AS c2, b2, a2
        |MATCH (d) RETURN d, c2, b2, a2 ORDER BY a2.prop, b2.prop""".stripMargin)

    interestingOrders(result).take(4) should be(List(
      InterestingOrder.ascInteresting("a.prop"),
      InterestingOrder.asc("  a2@20.prop").ascInteresting("  b2.prop"),
      InterestingOrder.ascInteresting("  a2@20.prop").ascInteresting("  b2.prop"),
      InterestingOrder.asc("  a2@20.prop").asc("  b2.prop")
    ))
  }

  test("Extracts required order from query returning multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop, n.foo ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.foo").desc("n.prop"),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop"), "n.foo" -> prop("n", "foo")),
        QueryShuffle(Seq(AscSortItem(varFor("n.foo"))(pos), DescSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.foo").desc("n.prop"),
      horizon = RegularQueryProjection(Map("n" -> varFor("n")),
        QueryShuffle(Seq(AscSortItem(prop("n", "foo"))(pos), DescSortItem(prop("n", "prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query returning some of multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.foo").desc("n.prop"),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "n.prop" -> prop("n", "prop")),
        QueryShuffle(Seq(AscSortItem(prop("n", "foo"))(pos), DescSortItem(varFor("n.prop"))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH and two ORDER BYs") {
    val result = buildPlannerQuery("MATCH (n) WITH n AS foo ORDER BY n.prop RETURN foo.bar ORDER BY foo.bar")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("foo" -> varFor("n")), QueryShuffle(Seq(AscSortItem(prop("foo", "prop"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo")),
        interestingOrder = InterestingOrder.asc("foo.bar"),
        horizon = RegularQueryProjection(Map("foo.bar" -> prop("foo", "bar")), QueryShuffle(Seq(AscSortItem(varFor("foo.bar"))(pos))))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable and returned") {
    val result = buildPlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar, foo ORDER BY foo")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "foo" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo", "n")),
        interestingOrder = InterestingOrder.asc("foo"),
        horizon = RegularQueryProjection(Map("n.bar" -> prop("n", "bar"), "foo" -> varFor("foo")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos))))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable") {
    val result = buildPlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar ORDER BY foo")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.asc("n.prop"),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "foo" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo", "n")),
        interestingOrder = InterestingOrder.asc("foo"),
        horizon = RegularQueryProjection(Map("n.bar" -> prop("n", "bar")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos))))
      ))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if order column is part of a more complex expression and the property is returned") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop * 2")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.empty,
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(Multiply(varFor("n.prop"), SignedDecimalIntegerLiteral("2")(pos))(pos))(pos))))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if order column is part of a more complex expression and the expression is returned") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop * 2 ORDER BY n.prop * 2")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.empty,
      horizon = RegularQueryProjection(Map("n.prop * 2" -> Multiply(prop("n", "prop"), SignedDecimalIntegerLiteral("2")(pos))(pos)), QueryShuffle(Seq(AscSortItem(varFor("n.prop * 2"))(pos))))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if second order column is part of a more complex expression") {
    val result = buildPlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop * 2 DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.empty,
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "n.prop" -> prop("n", "prop")),
        QueryShuffle(Seq(AscSortItem(prop("n", "foo"))(pos), DescSortItem(Multiply(varFor("n.prop"), SignedDecimalIntegerLiteral("2")(pos))(pos))(pos))))
    )

    result should equal(expectation)
  }

  test("Extracts property lookups even for dates") {
    val result = buildPlannerQuery("WITH date() AS d RETURN d.year ORDER BY d.year")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(),
      horizon = RegularQueryProjection(Map("d" -> ResolvedFunctionInvocation(QualifiedName(Seq.empty, "date"), None, IndexedSeq.empty)(pos))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("d")),
        interestingOrder = InterestingOrder.asc("d.year"),
        horizon = RegularQueryProjection(Map("d.year" -> prop("d", "year")), QueryShuffle(Seq(AscSortItem(varFor("d.year"))(pos))))
      ))
    )

    result should equal(expectation)
  }

  def interestingOrders(plannerQuery: PlannerQuery): List[InterestingOrder] =
    plannerQuery.tail match {
      case None => List(plannerQuery.interestingOrder)
      case Some(tail) => plannerQuery.interestingOrder :: interestingOrders(tail)
  }
}
