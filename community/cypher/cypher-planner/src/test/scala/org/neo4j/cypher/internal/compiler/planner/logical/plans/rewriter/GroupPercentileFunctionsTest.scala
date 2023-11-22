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

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
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

  test("should not rewrite two percentiles on different variables") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n, 0.5) AS p"))
      .projection("from.number AS n")
      .allNodeScan("from")
      .build()

    assertNotRewritten(before)
  }

  test("should rewrite two percentiles on same variable") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .aggregation(Seq.empty, Seq("percentileDisc(n, 0.5) AS p1", "percentileDisc(n, 0.6) AS p2"))
      .projection("from.number1 AS n")
      .allNodeScan("from")
      .build()

    val mapName = "   map0"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$mapName`.p1 AS p1", s"`$mapName`.p2 AS p2")
      .aggregation(
        Map.empty[String, Expression],
        Map(mapName -> multiPercentileDisc(varFor("n"), Seq(0.5, 0.6), Seq("p1", "p2")))
      )
      .projection("from.number1 AS n")
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
          mapName -> multiPercentileDisc(varFor("n1"), Seq(0.5, 0.6), Seq("p1", "p2")),
          "p3" -> function(PercentileDisc.name, varFor("n2"), literalFloat(0.7))
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
          "percentileDisc(n1, 0.5) AS p1",
          "percentileDisc(n1, 0.6) AS p2",
          "percentileDisc(n2, 0.7) AS p3",
          "percentileDisc(n2, 0.8) AS p4"
        )
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    val map0 = "   map0"
    val map1 = "   map1"

    val after = new LogicalPlanBuilder(wholePlan = false)
      .projection(s"`$map0`.p1 AS p1", s"`$map0`.p2 AS p2", s"`$map1`.p3 AS p3", s"`$map1`.p4 AS p4")
      .aggregation(
        Map.empty[String, Expression],
        Map(
          map0 -> multiPercentileDisc(varFor("n1"), Seq(0.5, 0.6), Seq("p1", "p2")),
          map1 -> multiPercentileDisc(varFor("n2"), Seq(0.7, 0.8), Seq("p3", "p4"))
        )
      )
      .projection("from.n1 AS n1", "from.n2 AS n2")
      .allNodeScan("from")
      .build()

    rewrite(before, names = Seq(map1, map0)) should equal(after)
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan, names: Seq[String] = Seq.empty): LogicalPlan =
    p.endoRewrite(groupPercentileFunctions(new VariableNameGenerator(names), new SequentialIdGen(initialValue = 0)))

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
