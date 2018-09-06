/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class RequiredOrderStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Extracts required order from query returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID19" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID19"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID19")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from distinct") {
    val result = buildPlannerQuery("MATCH (n) RETURN DISTINCT n.prop ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = DistinctQueryProjection(Map("  FRESHID28" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID28"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID28")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID28")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from aggregation") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop, count(*) ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = AggregatingQueryProjection(Map("  FRESHID19" -> prop("n", "prop")), Map("  FRESHID25" -> CountStar()(pos)), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID19"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19", "  FRESHID25")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID19"), "count(*)" -> varFor("  FRESHID25")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop2 ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID19" -> prop("n", "prop2")), QueryShuffle(Seq(AscSortItem(prop("n", "prop"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19")),
        horizon = RegularQueryProjection(Map("n.prop2" -> varFor("  FRESHID19")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order if variable is not projected") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop2 DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop2", DescColumnOrder))),
      horizon = RegularQueryProjection(projections = Map("  FRESHID19" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)),
        shuffle = QueryShuffle(List(DescSortItem(Property(Variable("n")(pos), PropertyKeyName("prop2")(pos))(pos))(pos)), None, None)),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19")),
        horizon = RegularQueryProjection(Map("n.prop" -> Variable("  FRESHID19")(pos))))))

    result should equal(expectation)
  }

  test("Extracts descending required order from query returning the sort column") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", DescColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID19" -> prop("n", "prop")), QueryShuffle(Seq(DescSortItem(varFor("  FRESHID19"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID19")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column, but a dependency") {
    val result = buildPlannerQuery("MATCH (n) RETURN n ORDER BY n.prop")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("  n@7")),
      requiredOrder = RequiredOrder(Seq(("  n@7.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID17" -> varFor("  n@7")), QueryShuffle(Seq(AscSortItem(prop("  FRESHID17", "prop"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID17")),
        horizon = RegularQueryProjection(Map("n" -> varFor("  FRESHID17")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query returning multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop, n.foo ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.foo", AscColumnOrder), ("n.prop", DescColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID19" -> prop("n", "prop"), "  FRESHID27" -> prop("n", "foo")),
        QueryShuffle(Seq(AscSortItem(varFor("  FRESHID27"))(pos), DescSortItem(varFor("  FRESHID19"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19", "  FRESHID27")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID19"), "n.foo" -> varFor("  FRESHID27")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("  n@7")),
      requiredOrder = RequiredOrder(Seq(("  n@7.foo", AscColumnOrder), ("  n@7.prop", DescColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID17" -> varFor("  n@7")),
        QueryShuffle(Seq(AscSortItem(prop("  FRESHID17", "foo"))(pos), DescSortItem(prop("  FRESHID17", "prop"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID17")),
        horizon = RegularQueryProjection(Map("n" -> varFor("  FRESHID17")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query returning some of multiple sort columns") {
    val result = buildPlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("  n@7")),
      requiredOrder = RequiredOrder(Seq(("  n@7.foo", AscColumnOrder), ("  n@7.prop", DescColumnOrder))),
      horizon = RegularQueryProjection(Map("  FRESHID17" -> varFor("  n@7"), "  FRESHID22" -> prop("  n@7", "prop")),
        QueryShuffle(Seq(AscSortItem(prop("  FRESHID17", "foo"))(pos), DescSortItem(varFor("  FRESHID22"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID17", "  FRESHID22")),
        horizon = RegularQueryProjection(Map("n" -> varFor("  FRESHID17"), "n.prop" -> varFor("  FRESHID22")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH and two ORDER BYs") {
    val result = buildPlannerQuery("MATCH (n) WITH n AS foo ORDER BY n.prop RETURN foo.bar ORDER BY foo.bar")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map("foo" -> varFor("n")), QueryShuffle(Seq(AscSortItem(prop("foo", "prop"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo")),
        requiredOrder = RequiredOrder(Seq(("foo.bar", AscColumnOrder))),
        horizon = RegularQueryProjection(Map("  FRESHID51" -> prop("foo", "bar")), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID51"))(pos)))),
        tail = Some(RegularPlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("  FRESHID51")),
          horizon = RegularQueryProjection(Map("foo.bar" -> varFor("  FRESHID51")))
        ))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable and returned") {
    val result = buildPlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar, foo ORDER BY foo")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map( "n" -> varFor("n"), "  foo@28" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("  foo@28"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  foo@28", "n")),
        requiredOrder = RequiredOrder(Seq(("  foo@28", AscColumnOrder))),
        horizon = RegularQueryProjection(Map("  FRESHID57" -> prop("n", "bar"), "  FRESHID62" -> varFor("  foo@28")), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID62"))(pos)))),
        tail = Some(RegularPlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("  FRESHID57", "  FRESHID62")),
          horizon = RegularQueryProjection(Map("n.bar" -> varFor("  FRESHID57"),  "foo" -> varFor("  FRESHID62")))
        ))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable") {
    val result = buildPlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar ORDER BY foo")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder(Seq(("n.prop", AscColumnOrder))),
      horizon = RegularQueryProjection(Map( "n" -> varFor("n"), "foo" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo", "n")),
        requiredOrder = RequiredOrder(Seq(("foo", AscColumnOrder))),
        horizon = RegularQueryProjection(Map("  FRESHID57" -> prop("n", "bar")), QueryShuffle(Seq(AscSortItem(varFor("foo"))(pos)))),
        tail = Some(RegularPlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("  FRESHID57")),
          horizon = RegularQueryProjection(Map("n.bar" -> varFor("  FRESHID57")))
        ))
      ))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if order column is part of a more complex expression and the property is returned") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop * 2")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder.empty,
      horizon = RegularQueryProjection(Map("  FRESHID19" -> prop("n", "prop")), QueryShuffle(Seq(AscSortItem(Multiply(varFor("  FRESHID19"), SignedDecimalIntegerLiteral("2")(pos))(pos))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID19")),
        horizon = RegularQueryProjection(Map("n.prop" -> varFor("  FRESHID19")))
      ))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if order column is part of a more complex expression and the expression is returned") {
    val result = buildPlannerQuery("MATCH (n) RETURN n.prop * 2 ORDER BY n.prop * 2")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      requiredOrder = RequiredOrder.empty,
      horizon = RegularQueryProjection(Map("  FRESHID24" -> Multiply(prop("n", "prop"), SignedDecimalIntegerLiteral("2")(pos))(pos)), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID24"))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID24")),
        horizon = RegularQueryProjection(Map("n.prop * 2" -> varFor("  FRESHID24")))
      ))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if second order column is part of a more complex expression") {
    val result = buildPlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop * 2 DESC")

    val expectation = RegularPlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("  n@7")),
      requiredOrder = RequiredOrder.empty,
      horizon = RegularQueryProjection(Map("  FRESHID17" -> varFor("  n@7"), "  FRESHID22" -> prop("  n@7", "prop")),
        QueryShuffle(Seq(AscSortItem(prop("  FRESHID17", "foo"))(pos), DescSortItem(Multiply(varFor("  FRESHID22"), SignedDecimalIntegerLiteral("2")(pos))(pos))(pos)))),
      tail = Some(RegularPlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("  FRESHID17", "  FRESHID22")),
        horizon = RegularQueryProjection(Map("n" -> varFor("  FRESHID17"), "n.prop" -> varFor("  FRESHID22")))
      ))
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
        requiredOrder = RequiredOrder(Seq(("d.year", AscColumnOrder))),
        horizon = RegularQueryProjection(Map("  FRESHID26" -> prop("d", "year")), QueryShuffle(Seq(AscSortItem(varFor("  FRESHID26"))(pos)))),
        tail = Some(RegularPlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set("  FRESHID26")),
          horizon = RegularQueryProjection(Map("d.year" -> varFor("  FRESHID26")))
        ))
      ))
    )

    result should equal(expectation)
  }
}
