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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class GroupPercentileFunctionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should not rewrite single percentile") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n, 0.5) AS p"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite single distinct percentile") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(DISTINCT n, 0.5) AS p"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite two percentileDisc on different variables") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n1, 0.5) AS p1", "percentileDisc(n2, 0.5) AS p2"))
      .projection("from.number AS n1", "from.number2 AS n2")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should not rewrite two percentileDisc on same variable when one is distinct") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n, 0.5) AS p1", "percentileDisc(DISTINCT n, 0.5) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should rewrite two percentileDisc on same variable") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n, 0.5) AS p1", "percentileDisc(n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true)))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should be case insensitive when rewriting percentileDisc") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("pErceNtilEDISc(n, 0.5) AS p1", "PercenTILEDisc(n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true)))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should be case insensitive when rewriting percentileCont") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("pErceNtilEConT(n, 0.5) AS p1", "PercenTILEcONt(n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(false, false)))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite two percentileDisc on same variable in ordered aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(
        Seq("from AS from"),
        Seq("percentileDisc(n, 0.5) AS p1", "percentileDisc(n, 0.6) AS p2"),
        Seq("from")
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .orderedAggregation(
        Map("from" -> v"from"),
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true))),
        Seq("from")
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite two distinct percentileDisc on same variable") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(DISTINCT n, 0.5) AS p1", "percentileDisc(DISTINCT n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true), distinct = true))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite two percentileDisc on same variable but not a third that is distinct") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq.empty,
        Seq("percentileDisc(n, 0.5) AS p1", "percentileDisc(n, 0.6) AS p2", "percentileDisc(DISTINCT n, 0.6) AS p3")
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(
          mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true)),
          "p3" -> distinctFunction(PercentileDisc.name, v"n", literalFloat(0.6))
        )
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should preserve argument order") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Map.empty[String, Expression],
        Map(
          "p1" -> function(PercentileDisc.name, ArgumentDesc, v"n", literalFloat(0.5)),
          "p2" -> function(PercentileDisc.name, ArgumentDesc, v"n", literalFloat(0.6))
        )
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true), order = ArgumentDesc))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite one percentileDisc and one percentileCont on same variable") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileCont(n, 0.5) AS p1", "percentileDisc(n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(false, true)))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite one percentileDisc and one percentileCont on same variable when distinct") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileCont(DISTINCT n, 0.5) AS p1", "percentileDisc(DISTINCT n, 0.6) AS p2"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(v"n", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(false, true), distinct = true))
      )
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite two percentileDisc on same property") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(from.number1, 0.5) AS p1", "percentileDisc(from.number1, 0.6) AS p2"))
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(prop("from", "number1"), Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true)))
      )
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite two percentileDisc on same property when distinct") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq.empty,
        Seq("percentileDisc(DISTINCT from.number1, 0.5) AS p1", "percentileDisc(DISTINCT from.number1, 0.6) AS p2")
      )
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> percentiles(
          prop("from", "number1"),
          Seq(0.5, 0.6),
          Seq("p1", "p2"),
          Seq(true, true),
          distinct = true
        ))
      )
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should only rewrite percentiles that work on same variable") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq.empty,
        Seq("percentileDisc(n1, 0.5) AS p1", "percentileDisc(n1, 0.6) AS p2", "percentileDisc(n2, 0.7) AS p3")
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(
          mapName -> percentiles(v"n1", Seq(0.5, 0.6), Seq("p1", "p2"), Seq(true, true)),
          "p3" -> function(PercentileDisc.name, v"n2", literalFloat(0.7))
        )
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(mapName)) should equal(after)
  }

  test("should rewrite when there are multiple groups") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(
        Seq.empty,
        Seq(
          "percentileDisc(DISTINCT n1, 0.5) AS p0",
          "percentileCont(DISTINCT n1, 0.6) AS p1",
          "percentileDisc(n1, 0.5) AS p2",
          "percentileDisc(n1, 0.6) AS p3",
          "percentileDisc(DISTINCT n2, 0.7) AS p4",
          "percentileDisc(n2, 0.7) AS p5",
          "percentileDisc(n2, 0.8) AS p6",
          "percentileCont(n2, 0.8) AS p7"
        )
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    val map0 = "   map0"
    val map1 = "   map1"
    val map2 = "   map2"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(
        s"`$map1`.p0 AS p0",
        s"`$map1`.p1 AS p1",
        s"`$map0`.p2 AS p2",
        s"`$map0`.p3 AS p3",
        s"`$map2`.p5 AS p5",
        s"`$map2`.p6 AS p6",
        s"`$map2`.p7 AS p7"
      )
      .aggregation(
        Map.empty[String, Expression],
        Map(
          map1 -> percentiles(v"n1", Seq(0.6, 0.5), Seq("p1", "p0"), Seq(false, true), distinct = true),
          map0 -> percentiles(v"n1", Seq(0.6, 0.5), Seq("p3", "p2"), Seq(true, true)),
          "p4" -> distinctFunction(PercentileDisc.name, v"n2", literalFloat(0.7)),
          map2 -> percentiles(v"n2", Seq(0.8, 0.7, 0.8), Seq("p6", "p5", "p7"), Seq(true, true, false))
        )
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(map1, map2, map0)) should equal(after)
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan, names: Seq[String] = Seq.empty): LogicalPlan = {
    p.endoRewrite(groupPercentileFunctions(
      new VariableNameGenerator(names),
      Attributes(idGen)
    ))
  }

  class VariableNameGenerator(names: Seq[String]) extends AnonymousVariableNameGenerator {
    private val namesIterator = names.iterator

    override def nextName: String = {
      if (namesIterator.hasNext) {
        namesIterator.next()
      } else {
        super.nextName
      }
    }
  }
}
