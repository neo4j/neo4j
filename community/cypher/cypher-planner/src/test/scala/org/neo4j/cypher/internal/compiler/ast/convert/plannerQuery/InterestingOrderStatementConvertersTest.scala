/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class InterestingOrderStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Extracts required order from query returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order from distinct") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN DISTINCT n.prop ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = DistinctQueryProjection(Map(v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test(s"Extracts interesting order from DISTINCT") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN DISTINCT n.prop, n.foo")
    result.interestingOrder shouldBe InterestingOrder(
      RequiredOrderCandidate(Seq.empty),
      Seq(
        InterestingOrderCandidate.asc(prop("n", "prop")),
        InterestingOrderCandidate.desc(prop("n", "prop")),
        InterestingOrderCandidate.asc(prop("n", "foo")),
        InterestingOrderCandidate.desc(prop("n", "foo"))
      )
    )
  }

  test(s"Extracts interesting node order from DISTINCT") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN DISTINCT n")
    result.interestingOrder shouldBe InterestingOrder(
      RequiredOrderCandidate(Seq.empty),
      Seq(
        InterestingOrderCandidate.asc(varFor("n")),
        InterestingOrderCandidate.desc(varFor("n"))
      )
    )
  }

  test("Extracts required order from aggregation") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, count(*) ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = AggregatingQueryProjection(
        Map(v"n.prop" -> prop("n", "prop")),
        Map(v"count(*)" -> CountStar()(pos)),
        isTerminating = true
      )
    )

    result should equal(expectation)
  }

  test(s"Extracts interesting order from aggregation") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, n.foo, count(*)")
    result.interestingOrder shouldBe InterestingOrder(
      RequiredOrderCandidate(Seq.empty),
      Seq(
        InterestingOrderCandidate.asc(prop("n", "prop")),
        InterestingOrderCandidate.desc(prop("n", "prop")),
        InterestingOrderCandidate.asc(prop("n", "foo")),
        InterestingOrderCandidate.desc(prop("n", "foo"))
      )
    )
  }

  test(s"Extracts interesting node order from aggregation") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n, count(*)")
    result.interestingOrder shouldBe InterestingOrder(
      RequiredOrderCandidate(Seq.empty),
      Seq(
        InterestingOrderCandidate.asc(varFor("n")),
        InterestingOrderCandidate.desc(varFor("n"))
      )
    )
  }

  test("Extracts interesting order from min") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop)")

    val func = min(prop("n", "prop"))
    val interestingOrderCandidate = InterestingOrderCandidate(Seq(Asc(prop("n", "prop"))))
    val interestingOrder = new InterestingOrder(RequiredOrderCandidate.empty, Seq(interestingOrderCandidate))
    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map(v"min(n.prop)" -> func), isTerminating = true)
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
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map(v"max(n.prop)" -> func), isTerminating = true)
    )

    result should equal(expectation)
    result.interestingOrder.interestingOrderCandidates should equal(Seq(interestingOrderCandidate))
  }

  test("Extracts interesting order from min order by min") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop) ORDER BY min(n.prop)")

    val func = min(prop("n", "prop"))
    val interestingOrderCandidate = InterestingOrderCandidate(Seq(Asc(prop("n", "prop"))))
    val interestingOrder =
      new InterestingOrder(RequiredOrderCandidate(Seq(Asc(varFor("min(n.prop)")))), Seq(interestingOrderCandidate))
    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = interestingOrder,
      horizon = AggregatingQueryProjection(Map.empty, Map(v"min(n.prop)" -> func), isTerminating = true)
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
         |ORDER BY min""".stripMargin
    )

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("n", "prop"))),
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("min"), Map(v"min" -> varFor("min"))))
    ))
  }

  test("Extracts required order from query not returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop2 ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"))),
      horizon = RegularQueryProjection(Map(v"n.prop2" -> prop("n", "prop2")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order if variable is not projected") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop2 DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate.desc(prop("n", "prop2"))),
      horizon = RegularQueryProjection(projections = Map(v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts descending required order from query returning the sort column") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.desc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning the sort column, but a dependency") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n ORDER BY n.prop")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"), Map(v"n" -> varFor("n")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Propagate interesting order to previous query graph") {
    val result =
      buildSinglePlannerQuery("MATCH (n) WITH n AS secretN MATCH (m) RETURN m, secretN ORDER BY secretN.prop")

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(
        prop("secretN", "prop"),
        Map(v"secretN" -> varFor("n"))
      )),
      InterestingOrder.required(RequiredOrderCandidate.asc(
        prop("secretN", "prop"),
        Map(v"secretN" -> varFor("secretN"))
      ))
    ))
  }

  test("Do not propagate unfulfillable order to previous query graph") {
    val result = buildSinglePlannerQuery(
      "MATCH (n) WITH n AS secretN MATCH (m) WHERE m.prop = secretN.prop RETURN m ORDER BY m.prop"
    )

    interestingOrders(result).take(2) should be(List(
      InterestingOrder.empty,
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("m", "prop"), Map(v"m" -> varFor("m"))))
    ))
  }

  test("Do not propagate interesting order over required order") {
    val result = buildSinglePlannerQuery(
      """MATCH (a) WITH a AS a2
        |MATCH (b) WITH b AS b2, a2 ORDER BY a2.prop
        |MATCH (c) WITH c AS c2, b2, a2
        |MATCH (d) RETURN d, c2, b2, a2 ORDER BY c2.prop""".stripMargin
    )

    interestingOrders(result).take(4) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("a2", "prop"), Map(v"a2" -> varFor("a")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("a2", "prop"), Map(v"a2" -> varFor("a2")))),
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("c2", "prop"), Map(v"c2" -> varFor("c")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("c2", "prop"), Map(v"c2" -> varFor("c2"))))
    ))
  }

  test("Extracts required order from query returning multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop, n.foo ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      InterestingOrder.required(RequiredOrderCandidate
        .asc(varFor("n.foo"), Map(v"n.foo" -> prop("n", "foo")))
        .desc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon =
        RegularQueryProjection(Map(v"n.prop" -> prop("n", "prop"), v"n.foo" -> prop("n", "foo")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order from query not returning multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map(v"n" -> varFor("n")))
        .desc(prop("n", "prop"), Map(v"n" -> varFor("n")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order from query returning some of multiple sort columns") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map(v"n" -> varFor("n")))
        .desc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n"), v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH and two ORDER BYs") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n AS foo ORDER BY n.prop RETURN foo.bar ORDER BY foo.bar")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(prop("foo", "prop"), Map(v"foo" -> varFor("n")))),
      horizon = RegularQueryProjection(Map(v"foo" -> varFor("n"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set(v"foo")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(
          varFor("foo.bar"),
          Map(v"foo.bar" -> prop("foo", "bar"))
        )),
        horizon = RegularQueryProjection(Map(v"foo.bar" -> prop("foo", "bar")), isTerminating = true)
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable and returned") {
    val result =
      buildSinglePlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar, foo ORDER BY foo")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map(v"foo" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n"), v"foo" -> prop("n", "prop"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set(v"foo", v"n")),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map(v"foo" -> varFor("foo")))),
        horizon =
          RegularQueryProjection(Map(v"n.bar" -> prop("n", "bar"), v"foo" -> varFor("foo")), isTerminating = true)
      ))
    )

    result should equal(expectation)
  }

  test("Extracts required order from query with WITH where a property lookup is projected to a variable") {
    val result = buildSinglePlannerQuery("MATCH (n) WITH n, n.prop AS foo ORDER BY n.prop RETURN n.bar ORDER BY foo")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"), Map(v"foo" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n"), v"foo" -> prop("n", "prop"))),
      tail = Some(RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = Set(v"foo", v"n")),
        interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("foo"))),
        horizon = RegularQueryProjection(Map(v"n.bar" -> prop("n", "bar")), isTerminating = true)
      ))
    )

    result should equal(expectation)
  }

  test("Extract required order if order column is part of a more complex expression and the property is returned") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop * 2")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(
        RequiredOrderCandidate.asc(
          multiply(varFor("n.prop"), literalInt(2)),
          Map(v"n.prop" -> prop("n", "prop"))
        )
      ),
      horizon = RegularQueryProjection(Map(v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test(
    "Does not extract required order if order column is part of a more complex expression and the expression is returned"
  ) {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n.prop * 2 ORDER BY n.prop * 2")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(
        RequiredOrderCandidate.asc(
          varFor("n.prop * 2"),
          Map(v"n.prop * 2" -> multiply(prop("n", "prop"), literalInt(2)))
        )
      ),
      horizon =
        RegularQueryProjection(Map(v"n.prop * 2" -> multiply(prop("n", "prop"), literalInt(2))), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts required order even if second order column is part of a more complex expression") {
    val result = buildSinglePlannerQuery("MATCH (n) RETURN n, n.prop ORDER BY n.foo, n.prop * 2 DESC")

    val expectation = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(patternNodes = Set(v"n")),
      interestingOrder = InterestingOrder.required(RequiredOrderCandidate
        .asc(prop("n", "foo"), Map(v"n" -> varFor("n")))
        .desc(multiply(varFor("n.prop"), literalInt(2)), Map(v"n.prop" -> prop("n", "prop")))),
      horizon = RegularQueryProjection(Map(v"n" -> varFor("n"), v"n.prop" -> prop("n", "prop")), isTerminating = true)
    )

    result should equal(expectation)
  }

  test("Extracts property lookups even for dates") {
    val dateName = QualifiedName(Seq.empty, "date")
    val dateSignature = Some(UserFunctionSignature(
      name = dateName,
      inputSignature = IndexedSeq.empty,
      outputType = CTDate,
      deprecationInfo = None,
      description = None,
      isAggregate = false,
      id = 12,
      builtIn = false
    ))
    val result = buildSinglePlannerQuery(
      "WITH date() AS d RETURN d.year ORDER BY d.year",
      functionLookup = Some(Map(dateName -> dateSignature))
    )

    val expectedHorizon = RegularQueryProjection(Map(v"d" -> ResolvedFunctionInvocation(
      QualifiedName(Seq.empty, "date"),
      dateSignature,
      IndexedSeq.empty
    )(pos)))
    val expectedTail = Some(RegularSinglePlannerQuery(
      queryGraph = QueryGraph(argumentIds = Set(v"d")),
      interestingOrder =
        InterestingOrder.required(RequiredOrderCandidate.asc(varFor("d.year"), Map(v"d.year" -> prop("d", "year")))),
      horizon = RegularQueryProjection(Map(v"d.year" -> prop("d", "year")), isTerminating = true)
    ))

    result.horizon should equal(expectedHorizon)
    result.tail should equal(expectedTail)
  }

  test("should not find required order when there is none, single query part") {
    val q = buildSinglePlannerQuery("MATCH (n) RETURN n.prop")
    q.findFirstRequiredOrder shouldBe empty
  }

  test("should not find required order when there is none, multiple query parts") {
    val q = buildSinglePlannerQuery("MATCH (n) WITH DISTINCT n RETURN n.prop")
    q.findFirstRequiredOrder shouldBe empty
  }

  test("should not find required order when it's not usable by the head query part") {
    val q = buildSinglePlannerQuery(
      "MATCH (n) WITH DISTINCT n MATCH (n)--(otherNode) RETURN otherNode.prop ORDER BY otherNode.prop"
    )
    q.findFirstRequiredOrder shouldBe empty
  }

  test("should not find required order when it's not usable by the head query part because of aggregation") {
    val q = buildSinglePlannerQuery("MATCH (n) WITH n, 1 AS foo RETURN n, count(*) AS c ORDER BY c")
    q.findFirstRequiredOrder shouldBe empty

    q.interestingOrder.requiredOrderCandidate shouldEqual RequiredOrderCandidate.empty
    q.tail.get.interestingOrder.requiredOrderCandidate shouldNot equal(RequiredOrderCandidate.empty)
  }

  test("should not find UNWIND required order when it's not usable by the head query part because of aggregation") {
    val q = buildSinglePlannerQuery("UNWIND [1, 2, 3] AS n RETURN n, count(*) AS c ORDER BY c")
    q.findFirstRequiredOrder shouldBe empty

    q.interestingOrder.requiredOrderCandidate shouldEqual RequiredOrderCandidate.empty
    q.tail.get.interestingOrder.requiredOrderCandidate shouldNot equal(RequiredOrderCandidate.empty)
  }

  test("should find UNWIND required order when it's usable by the head query part") {
    val q = buildSinglePlannerQuery("MATCH (c) UNWIND [1, 2, 3] AS n RETURN n, c ORDER BY c")
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("c"), Map(v"c" -> varFor("c"))))
    )

    interestingOrders(q).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("c"), Map(v"c" -> varFor("c")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("c"), Map(v"c" -> varFor("c"))))
    ))
  }

  test("should find required order when it's usable only by aggregating horizon but not by MATCH part before it") {
    val q = buildSinglePlannerQuery("""MATCH (a)
                                      |WITH count(a) AS count
                                      |MATCH (b)-[r2:R]->(c)
                                      |RETURN b, c, count ORDER BY count""".stripMargin)
    // Ideally, we would want a different Interesting order for the `MATCH (a)` and for the `WITH count(a) AS count`.
    // In the MATCH part we cannot yet sort by count, but in the WITH part we can.
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(
        varFor("count"),
        Map(v"count" -> function("count", varFor("a")))
      ))
    )

    interestingOrders(q) should be(List(
      // Interesting order candidate found in the first query part. This is translated back to the function invocation.
      InterestingOrder.interested(InterestingOrderCandidate.asc(
        varFor("count"),
        Map(v"count" -> function("count", varFor("a")))
      )),
      // Required order candidate found in the second query part. This is not aware of the function in the first part's horizon.
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("count"), Map(v"count" -> varFor("count"))))
    ))
  }

  test("should find UNWIND property required order when it's usable by the head query part") {
    val q = buildSinglePlannerQuery(
      """
        |MATCH (n:N)-[r:R]-() WHERE n.prop IS NOT NULL
        |UNWIND [1, 2, 3] AS i
        |RETURN n
        |ORDER BY n.prop""".stripMargin
    )
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"), Map(v"n" -> varFor("n"))))
    )

    interestingOrders(q).take(2) should be(List(
      InterestingOrder.interested(InterestingOrderCandidate.asc(prop("n", "prop"), Map(v"n" -> varFor("n")))),
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"), Map(v"n" -> varFor("n"))))
    ))
  }

  test("should find required order in head query part") {
    val q = buildSinglePlannerQuery("MATCH (n) RETURN n.prop ORDER BY n.prop")
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("n.prop"), Map(v"n.prop" -> prop("n", "prop"))))
    )
  }

  test("should find required order in tail") {
    val q = buildSinglePlannerQuery("MATCH (n) WITH DISTINCT n RETURN n.prop ORDER BY n.prop")
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("n", "prop"), Map(v"n" -> varFor("n"))))
    )
  }

  test("should find required order in tail with aliasing") {
    val q = buildSinglePlannerQuery("MATCH (n) WITH DISTINCT n AS x RETURN x.prop ORDER BY x.prop")
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "prop"), Map(v"x" -> varFor("n"))))
    )
  }

  test("should find required order in tail with multiple aliases") {
    val q = buildSinglePlannerQuery {
      """MATCH (n)
        |WITH DISTINCT n AS x
        |WITH x AS y SKIP 0
        |RETURN y.prop
        |ORDER BY y.prop""".stripMargin
    }
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "prop"), Map(v"x" -> varFor("n"))))
    )
  }

  test("should find required order prefix in tail with multiple columns") {
    val q = buildSinglePlannerQuery {
      """MATCH (n)
        |WITH DISTINCT n AS x
        |MATCH (x)-->(y)
        |RETURN x, y
        |ORDER BY x.prop, x.otherProp, y.yetAnotherProp""".stripMargin
    }
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(
        RequiredOrderCandidate
          .asc(prop("x", "prop"), Map(v"x" -> varFor("n")))
          .asc(prop("x", "otherProp"), Map(v"x" -> varFor("n")))
      )
    )
  }

  test("should not find required order in tail when the first ordering column is not usable") {
    val q = buildSinglePlannerQuery {
      """MATCH (n)
        |WITH DISTINCT n AS x
        |MATCH (x)-->(y)
        |RETURN x, y
        |ORDER BY y.prop, x.prop""".stripMargin
    }
    q.findFirstRequiredOrder shouldBe empty
  }

  test("should find required order prefix in tail until the first unusable column only") {
    val q = buildSinglePlannerQuery {
      """MATCH (n)
        |WITH DISTINCT n AS x
        |MATCH (x)-->(y)
        |RETURN x, y
        |ORDER BY x.prop, y.otherProp, x.yetAnotherProp""".stripMargin
    }
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(
        RequiredOrderCandidate
          .asc(prop("x", "prop"), Map(v"x" -> varFor("n")))
      )
    )
  }

  test("should propagate required order beyond aggregating horizon") {
    val q = buildSinglePlannerQuery(
      """MATCH (a)
        |WITH a.name as name,
        |   count(a) AS count
        |RETURN name, count
        |  ORDER BY name, count""".stripMargin
    )
    q.findFirstRequiredOrder shouldBe Some(
      InterestingOrder.required(RequiredOrderCandidate
        .asc(varFor("name"), Map(v"name" -> prop(varFor("a"), "name")))
        .asc(varFor("count"), Map(v"count" -> function("count", varFor("a")))))
    )
  }

  private def interestingOrders(plannerQuery: SinglePlannerQuery): List[InterestingOrder] =
    plannerQuery.tail match {
      case None       => List(plannerQuery.interestingOrder)
      case Some(tail) => plannerQuery.interestingOrder :: interestingOrders(tail)
    }
}
