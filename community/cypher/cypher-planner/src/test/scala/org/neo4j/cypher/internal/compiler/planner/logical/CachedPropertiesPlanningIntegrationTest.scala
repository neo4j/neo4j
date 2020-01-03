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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, CartesianProduct, Expand, Projection, Selection}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.v4_0.expressions.{AndedPropertyInequalities, InequalityExpression}
import org.neo4j.cypher.internal.v4_0.util.NonEmptyList
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class CachedPropertiesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should cache node property on multiple usages") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 RETURN n.prop1")

    plan._2 should equal(
      Projection(
        Selection(Seq(greaterThan(cachedNodeProp("n", "prop1"), literalInt(42))),
          AllNodesScan("n", Set.empty)),
        Map("n.prop1" -> cachedNodeProp("n", "prop1"))
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

  // Note: This is only the right behavior since AndedPropertyComparablePredicates re-reads the property stupidly
  test("should rewrite node property if there are two usages in AndedPropertyInequalities") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 AND n.prop1 < 100 RETURN n")

    plan._2 should equal(
      Selection(Seq(cachedAndedNodePropertyInequalities("n", "prop1",
        lessThan(cachedNodeProp("n", "prop1"), literalInt(100)),
        greaterThan(cachedNodeProp("n", "prop1"), literalInt(42)))
      ),
        AllNodesScan("n", Set.empty))    )
  }

  test("should cache relationship property on multiple usages") {
    val plan = planFor("MATCH (a)-[r]-(b) WHERE r.prop1 > 42 RETURN r.prop1")

    plan._2 should equal(
      Projection(
        Selection(Seq(greaterThan(cachedRelProp("r", "prop1"), literalInt(42))),
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
        Selection(Seq(greaterThan(cachedNodeProp("n", "prop1"), literalInt(42))),
          AllNodesScan("n", Set.empty)),
          Map("x" -> varFor("n"))),
        Map("x.prop1" -> cachedNodeProp("n", "prop1"))
      )
    )
  }

  test("should cache renamed variable: n AS x with predicate in between") {
    val plan = planFor("MATCH (n) WHERE n.prop1 > 42 WITH n AS x WHERE x.prop1 > 42 RETURN x")

    plan._2 should equal(
      Selection(Seq(greaterThan(cachedNodeProp("n", "prop1"), literalInt(42))),
        Projection(
          Selection(Seq(greaterThan(cachedNodeProp("n", "prop1"), literalInt(42))),
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
            Selection(Seq(greaterThan(cachedNodeProp("n", "prop1"), literalInt(42))),
              AllNodesScan("n", Set.empty)),
          Selection(Seq(greaterThan(cachedNodeProp("  m@12", "prop1"), literalInt(42))),
            AllNodesScan("  m@12", Set.empty))),
          Map("  m@61" -> varFor("n"), "x" -> varFor("  m@12"))),
        Map("m.prop1" -> cachedNodeProp("n", "prop1"), "x.prop1" -> cachedNodeProp("  m@12", "prop1"))
      )
    )
  }

  private def cachedAndedNodePropertyInequalities(varName: String, propName: String, expression: InequalityExpression*) = {
    AndedPropertyInequalities(varFor(varName), cachedNodeProp(varName, propName), NonEmptyList(expression.head, expression.tail: _*))
  }
}
