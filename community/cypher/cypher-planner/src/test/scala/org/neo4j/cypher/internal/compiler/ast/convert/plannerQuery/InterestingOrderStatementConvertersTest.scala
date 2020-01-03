/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.InterestingOrder.{Asc, Desc}
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.v4_0.expressions.CountStar
import org.neo4j.cypher.internal.logical.plans.{QualifiedName, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class InterestingOrderStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Extracts required order from query returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from distinct") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN DISTINCT n.prop ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = DistinctQueryProjection(Map("n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from aggregation") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, count(*) ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = AggregatingQueryProjection(Map("n.prop" -> prop("n", "prop")), Map("count(*)" -> CountStar()(pos)))
    )

    result should equal(expectation)
  }

  test("Extracts interesting order from min") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop)")

    val func = min(prop("n", "prop"))
    val interestingOrderCandidate = InterestingOrderCandidate(Seq(Asc(prop("n", "prop"))))
    val interestingOrder = new InterestingOrder(RequiredOrderCandidate.empty, Seq(interestingOrderCandidate))
    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map("min(n.prop)" -> func))
    )

    result should equal(expectation)
    result.interestingOrder.interestingOrderCandidates should equal(Seq(interestingOrderCandidate))
  }

  test("Extracts interesting order from max") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN max(n.prop)")

    val func = max(prop("n", "prop"))
    val interestingOrderCandidate = InterestingOrderCandidate(Seq(Desc(prop("n", "prop"))))
    val interestingOrder = new InterestingOrder(RequiredOrderCandidate.empty, Seq(interestingOrderCandidate))
    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map("max(n.prop)" -> func))
    )

    result should equal(expectation)
    result.interestingOrder.interestingOrderCandidates should equal(Seq(interestingOrderCandidate))
  }

  test("Extracts interesting order from min order by min") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop) ORDER BY min(n.prop)")

    val func = min(prop("n", "prop"))
    val interestingOrderCandidate = InterestingOrderCandidate(Seq(Asc(prop("n", "prop"))))
    val interestingOrder = new InterestingOrder(RequiredOrderCandidate(Seq(Asc(varFor("min(n.prop)")))), Seq(interestingOrderCandidate))
    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map("min(n.prop)" -> func))
    )

    result should equal(expectation)
    result.interestingOrder.interestingOrderCandidates should equal(Seq(interestingOrderCandidate))
  }

  test(s"Interesting order for min(n.prop) in WITH and required order for ORDER BY min") {
    val result = buildSinglePlannerQuery(
      s"""MATCH (n:Awesome)
         |WHERE n.prop > 0
         |WITH min(n.prop) AS min
         |RETURN min
         |ORDER BY min""".stripMargin)

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("n", "prop"))),
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("min"), Map("min" -> varFor("min"))))
    ))
  }

  test("Extracts required order from query not returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop2 ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"))),
      horizon = RegularQueryProjection(Map("n.prop2" -> prop("n", "prop2")))
    )

    result should equal(expectation)
  }

  test("Extracts required order if variable is not projected") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop2 DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(prop("n", "prop2"))),
      horizon = RegularQueryProjection(projections = Map("n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts descending required order from query returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column, but a dependency") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"), Map("n" -> varFor("n")))),
      horizon = RegularQueryProjection(Map("n" -> varFor("n")))
    )

    result should equal(expectation)
  }

  test("Propagate interesting order to previous query graph") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n AS secretN MATCH (m) RETURN m, secretN ORDER BY secretN.prop")

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("secretN", "prop"), Map("secretN" -> varFor("n")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("secretN", "prop"), Map("secretN" -> varFor("secretN"))))
    ))
  }

  test("Do not propagate unfulfillable order to previous query graph") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n AS secretN MATCH (m) RETURN m ORDER BY m.prop")

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.empty,
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("m", "prop"), Map("m" -> varFor("m"))))
    ))
  }

  test("Do not propagate interesting order over required order") {
    val result = buildSinglePlannerQuery(
      """MATCH (a) WITH a AS a2
        |MATCH (b) WITH b AS b2, a2 ORDER BY a2.prop
        |MATCH (c) WITH c AS c2, b2, a2
        |MATCH (d) RETURN d, c2, b2, a2 ORDER BY c2.prop""".stripMargin)

    interestingOrders(result).take(4) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("a2", "prop"), Map("a2" -> varFor("a")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("a2", "prop"), Map("a2" -> varFor("a2")))),
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("c2", "prop"), Map("c2" -> varFor("c")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("c2", "prop"), Map("c2" -> varFor("c2"))))
    ))
  }

  ignore("Propagate suffix of interesting order if the interesting prefix overlaps the required order") {
    val result = buildSinglePlannerQuery(
      """MATCH (a) WITH a AS a2
        |MATCH (b) WITH b AS b2, a2 ORDER BY a2.prop
        |MATCH (c) WITH c AS c2, b2, a2
        |MATCH (d) RETURN d, c2, b2, a2 ORDER BY a2.prop, b2.prop""".stripMargin)

    interestingOrders(result).take(4) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("a2", "prop"), Map("a2" -> varFor("a")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("a2", "prop"), Map("a2" -> varFor("a2"))))
        .interesting(InterestingOrderCandidate.asc(prop("b2", "prop"), Map("b2" -> varFor("b")))),
      InterestingOrder.interested(InterestingOrderCandidate
        .asc(prop("a2", "prop"), Map("a2" -> varFor("a2"))).asc(prop("b2", "prop"), Map("b2" -> varFor("b2")))),
      InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("a2", "prop"), Map("a2" -> varFor("a2"))).asc(prop("b2", "prop"), Map("b2" -> varFor("b2"))))
    ))
  }

  test("Extracts required order from query returning multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, n.foo ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      InterestingOrder.required(RequiredOrderCandidate
        .asc(varFor("n.foo"), Map("n.foo" -> prop("n", "foo")))
        .desc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop"), "n.foo" -> prop("n", "foo")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map("n" -> varFor("n")))
        .desc(prop("n", "prop"), Map("n" -> varFor("n")))),
      horizon = RegularQueryProjection(Map("n" -> varFor("n")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query returning some of multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map("n" -> varFor("n")))
        .desc(varFor("n.prop"), Map("n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH and two ORDER BYs") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n AS foo ORDER BY n.prop RETURN foo.bar ORDER BY foo.bar")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("foo", "prop"), Map("foo" -> varFor("n")))),
      horizon = RegularQueryProjection(Map("foo" -> varFor("n"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo.bar"), Map("foo.bar" -> prop("foo", "bar")))),
        horizon = RegularQueryProjection(Map("foo.bar" -> prop("foo", "bar")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable and returned") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar, foo ORDER BY foo")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map("foo" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "foo" -> prop("n", "prop"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo", "n")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map("foo" -> varFor("foo")))),
        horizon = RegularQueryProjection(Map("n.bar" -> prop("n", "bar"), "foo" -> varFor("foo")))
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar ORDER BY foo")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map("foo" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "foo" -> prop("n", "prop"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("foo", "n")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"))),
        horizon = RegularQueryProjection(Map("n.bar" -> prop("n", "bar")))
      ))
    )

    result should equal(expectation)
  }

  test("Extract required order if order column is part of a more complex expression and the property is returned") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop * 2")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(
        RequiredOrderCandidate.asc(
          multiply(varFor("n.prop"), literalInt(2)),
          Map("n.prop" -> prop("n", "prop"))
        )
      ),
      horizon = RegularQueryProjection(Map("n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Does not extract required order if order column is part of a more complex expression and the expression is returned") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop * 2 ORDER BY n.prop * 2")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(
        RequiredOrderCandidate.asc(varFor("n.prop * 2"),
          Map("n.prop * 2" -> multiply(prop("n", "prop"), literalInt(2)))
        )
      ),
      horizon = RegularQueryProjection(Map("n.prop * 2" -> multiply(prop("n", "prop"), literalInt(2))))
    )

    result should equal(expectation)
  }

  test("Extracts required order even if second order column is part of a more complex expression") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop * 2 DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set("n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map("n" -> varFor("n")))
        .desc(multiply(varFor("n.prop"), literalInt(2)), Map("n.prop" -> prop("n", "prop")))
      ),
      horizon = RegularQueryProjection(Map("n" -> varFor("n"), "n.prop" -> prop("n", "prop")))
    )

    result should equal(expectation)
  }

  test("Extracts property lookups even for dates") {
    val result = buildSinglePlannerQuery("WITH date() AS d RETURN d.year ORDER BY d.year")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(),
      horizon = RegularQueryProjection(Map("d" -> ResolvedFunctionInvocation(QualifiedName(Seq.empty, "date"), None, IndexedSeq.empty)(pos))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set("d")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("d.year"), Map("d.year" -> prop("d", "year")))),
        horizon = RegularQueryProjection(Map("d.year" -> prop("d", "year")))
      ))
    )

    result should equal(expectation)
  }

  def interestingOrders(plannerQuery: SinglePlannerQuery): List[InterestingOrder] =
    plannerQuery.tail match {
      case None => List(plannerQuery.interestingOrder)
      case Some(tail) => plannerQuery.interestingOrder :: interestingOrders(tail)
  }
}
