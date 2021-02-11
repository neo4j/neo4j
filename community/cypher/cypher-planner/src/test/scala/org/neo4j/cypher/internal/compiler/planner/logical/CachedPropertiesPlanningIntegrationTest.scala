/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CachedPropertiesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with LogicalPlanningTestSupport2 {

  test("should cache node property on multiple usages") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 RETURN n.prop1")

    plan._2 should equal(
      Projection(
        Selection(Seq(greaterThan(cachedNodePropFromStore("n", "prop1"), literalInt(42))),
          AllNodesScan("n", Set.empty)),
        Map("n.prop1" -> cachedNodeProp("n", "prop1"))
      )
    )
  }

  test("should cache node property on multiple usages without return") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 SET n.prop2 = n.prop1")

    plan._2 should equal(
      EmptyResult(
        SetNodeProperty(
          Selection(Seq(greaterThan(cachedNodePropFromStore("n", "prop1"), literalInt(42))),
            AllNodesScan("n", Set.empty)),
          "n", PropertyKeyName("prop2")(pos), cachedNodeProp("n", "prop1")
        )
      )
    )
  }

  test("should not rewrite node property if there is only one usage") {
    val plan = planFor("MATCH (n) RETURN n.prop1")

    plan._2 should equal(
      Projection(
        AllNodesScan("n", Set.empty),
        Map("n.prop1" -> prop("n", "prop1"))
      )
    )
  }

  test("should not rewrite node property if there is only one usage in selection") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 RETURN n")

    plan._2 should equal(
      Selection(Seq(greaterThan(prop("n", "prop1"), literalInt(42))),
        AllNodesScan("n", Set.empty))    )
  }

  test("should cache relationship property on multiple usages") {
    val plan = planFor("MATCH (a)-[r]-(b) WHERE r.prop1 > 42 RETURN r.prop1")

    plan._2 should equal(
      Projection(
        Selection(Seq(greaterThan(cachedRelPropFromStore("r", "prop1"), literalInt(42))),
          Expand(
            AllNodesScan("a", Set.empty),
            "a", BOTH, Seq.empty, "b", "r")),
        Map("r.prop1" -> cachedRelProp("r", "prop1"))
      )
    )
  }

  test("should not rewrite relationship property if there is only one usage") {
    val plan = planFor("MATCH (a)-[r]-(b) RETURN r.prop1")

    plan._2 should equal(
      Projection(
        Expand(
          AllNodesScan("a", Set.empty),
          "a", BOTH, Seq.empty, "b", "r"),
        Map("r.prop1" -> prop("r", "prop1"))
      )
    )
  }

  test("should cache renamed variable: n AS x") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 WITH n AS x RETURN x.prop1")

    plan._2 should equal(
      Projection(
        Projection(
          Selection(Seq(greaterThan(cachedNodePropFromStore("n", "prop1"), literalInt(42))),
            AllNodesScan("n", Set.empty)),
          Map("x" -> varFor("n"))),
        Map("x.prop1" -> cachedNodeProp("n", "prop1", "x"))
      )
    )
  }

  test("should cache renamed variable: n AS x with predicate in between") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 WITH n AS x WHERE x.prop1 > 42 RETURN x")

    plan._2 should equal(
      Selection(Seq(greaterThan(cachedNodeProp("n", "prop1", "x"), literalInt(42))),
        Projection(
          Selection(Seq(greaterThan(cachedNodePropFromStore("n", "prop1"), literalInt(42))),
            AllNodesScan("n", Set.empty)),
          Map("x" -> varFor("n"))))
    )
  }

  test("should cache with byzantine renaming: n AS m, m AS x") {
    val plan = planFor("MATCH (n), (m) WHERE n.prop1 > 42 AND m.prop1 > 42 WITH n AS m, m AS x RETURN m.prop1, x.prop1")

    plan._2 should equal(
      Projection(
        Projection(
          CartesianProduct(
            Selection(Seq(greaterThan(cachedNodePropFromStore("n", "prop1"), literalInt(42))),
              AllNodesScan("n", Set.empty)),
            Selection(Seq(greaterThan(cachedNodePropFromStore("m", "prop1"), literalInt(42))),
              AllNodesScan("m", Set.empty))),
          Map("m" -> varFor("n"), "x" -> varFor("m"))),
        Map("m.prop1" -> cachedNodeProp("n", "prop1", "m"), "x.prop1" -> cachedNodeProp("m", "prop1", "x"))
      )
    )
  }

  test("should not push down property reads into RHS of apply unnecessarily") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1023)
      .setLabelCardinality("N", 12)
      .setLabelCardinality("M", 11)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .setRelationshipCardinality("(:N)-[]->(:M)", 2)
      .setRelationshipCardinality("()-[]->(:M)", 2)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)
        |CALL {
        |  WITH n
        |  MATCH (n)-->(m:M)
        |  CALL {
        |    WITH n
        |    MATCH (n)-->(o:M)
        |    RETURN o
        |  }
        |  RETURN m, o
        |}
        |RETURN m.prop
        |""".stripMargin)

    val cachePropertyPlans = plan.treeCount {
      case _: CacheProperties => true
    }

    withClue(plan) {
      cachePropertyPlans should be(0)
    }
  }

  test("should push down property reads past a LIMIT if work is reduced by the LIMIT") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("N", 500)
      .setRelationshipCardinality("()-[]->()", 1000)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)-[rel]->(m)
        |WITH * LIMIT 10
        |RETURN n.prop AS foo
        |""".stripMargin)

    plan shouldEqual cfg.planBuilder()
      .produceResults("foo")
      .projection("cacheN[n.prop] AS foo") // 10 rows
      .limit(10) // 10 rows
      .expandAll("(n)-[rel]->(m)") // 1000 rows, effective 10
      .cacheProperties("cacheNFromStore[n.prop]")
      .nodeByLabelScan("n", "N") // 500 rows, effective 5
      .build()
  }
}
