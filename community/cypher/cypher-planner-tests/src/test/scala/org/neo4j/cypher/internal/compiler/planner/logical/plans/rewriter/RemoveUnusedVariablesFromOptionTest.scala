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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.scalatest.Assertion

class RemoveUnusedVariablesFromOptionTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("should remove unused variables from an optional and argument") {
    val original = new LogicalPlanBuilder()
      .produceResults("c")
      .apply()
      .|.optional("a", "b", "c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("a", "b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("c")
      .apply()
      .|.optional("c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("should identify usages on the RHS of an apply") {
    val original = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.optional("a", "b", "c", "d")
      .|.|.filter(equals(prop("c", "prop"), prop("d", "prop")))
      .|.|.expand("(c)--(d)")
      .|.|.argument("a", "b", "c")
      .|.optional("a", "b", "c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("a", "b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.optional("d")
      .|.|.filter(equals(prop("c", "prop"), prop("d", "prop")))
      .|.|.expand("(c)--(d)")
      .|.|.argument("c")
      .|.optional("c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("should identify usages on leaf plans like nodeIndexOperator") {
    val original = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.optional("a", "b", "c", "d")
      .|.|.nodeIndexOperator("d:A(prop = cacheN[c.prop])", argumentIds = Set("a", "b", "c"))
      .|.optional("a", "b", "c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("a", "b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.optional("d")
      .|.|.nodeIndexOperator("d:A(prop = cacheN[c.prop])", argumentIds = Set("c"))
      .|.optional("c")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)--(c)")
      .|.argument("b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("should identify usages on leaf plans like relationshiplogicalleafplan") {
    val original = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.filter("rel.prop=rel2.prop")
      .|.|.relationshipTypeScan("(c)-[rel2:REL]-(d)", "a", "b", "c", "rel")
      .|.optional("a", "b", "c", "rel")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)-[rel]-(c)")
      .|.argument("a", "b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults("prop")
      .projection("d.prop AS prop")
      .apply()
      .|.apply()
      .|.|.filter("rel.prop=rel2.prop")
      .|.|.relationshipTypeScan("(c)-[rel2:REL]-(d)", "c", "rel")
      .|.optional("c", "rel")
      .|.filter(propEquality("c", "prop", 42))
      .|.expand("(b)-[rel]-(c)")
      .|.argument("b")
      .expand("(a)--(b)")
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("leave empty argument if all variables unused") {
    val original = new LogicalPlanBuilder()
      .produceResults()
      .apply()
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults()
      .apply()
      .|.argument()
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("leave empty optional if all variables unused") {
    val original = new LogicalPlanBuilder()
      .produceResults()
      .apply()
      .|.optional("a", "b")
      .|.expand("(a)--(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val expected = new LogicalPlanBuilder()
      .produceResults()
      .apply()
      .|.optional()
      .|.expand("(a)--(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    shouldRewrite(original, expected)
  }

  test("should not remove variables used in filters") {
    val original = new LogicalPlanBuilder()
      .produceResults()
      .apply()
      .|.filter(propEquality("p", "prop", 42))
      .|.argument("p")
      .nodeByLabelScan("p", "Person")
      .build()

    shouldNotRewrite(original)
  }

  def shouldNotRewrite(origin: LogicalPlan): Assertion =
    rewrite(origin) should equal(origin)

  def shouldRewrite(origin: LogicalPlan, target: LogicalPlan): Assertion =
    rewrite(origin) should equal(target)

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(RemoveUnusedVariablesFromOption)
}
