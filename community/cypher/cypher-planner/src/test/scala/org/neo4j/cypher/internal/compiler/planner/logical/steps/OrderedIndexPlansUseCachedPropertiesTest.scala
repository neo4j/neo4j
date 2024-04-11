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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderedIndexPlansUseCachedPropertiesTest extends CypherFunSuite {

  test("should accept if cached property from ordered node index plan is only used cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("cache[a.prop] AS p")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if non-cached property from unordered node index plan is used non-cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("a.prop AS p")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> DoNotGetValue), IndexOrderNone)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should not accept if non-cached property from ordered node index plan is used non-cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("a.prop AS p")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(Seq("a.prop should not appear non-cached."))
  }

  test("should accept if non-cached property from ordered node index plan is not used") {
    val plan = new LogicalPlanBuilder(false)
      .projection("123 AS p")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if cached property from ordered node index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if non-cached property from unordered node index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> DoNotGetValue), IndexOrderNone)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should not accept if non-cached property from ordered node index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(
      Seq(s"${plan.leftmostLeaf.toString} does not cache a.prop, but the entity is returned in ProduceResult.")
    )
  }

  test("should accept if non-cached property from ordered node index plan is used in UpdatePlan") {
    // SET, potentially nested in MERGE or FOREACH, is allowed to skip using a caching property.
    // In these cases we anyway will plan a Sort, so we don't run the risk of returning out-of-order results.
    val plan = new LogicalPlanBuilder(false)
      .setRelationshipProperty("a", "prop", "a.prop + 1")
      .nodeIndexOperator("a:A(prop)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if cached property from ordered relationship index plan is only used cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("cache[a.prop] AS p")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if non-cached property from unordered relationship index plan is used non-cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("a.prop AS p")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> DoNotGetValue), IndexOrderNone)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should not accept if non-cached property from ordered relationship index plan is used non-cached") {
    val plan = new LogicalPlanBuilder(false)
      .projection("a.prop AS p")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(Seq("a.prop should not appear non-cached."))
  }

  test("should accept if non-cached property from ordered relationship index plan is not used") {
    val plan = new LogicalPlanBuilder(false)
      .projection("123 AS p")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if cached property from ordered relationship index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> GetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should accept if non-cached property from unordered relationship index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> DoNotGetValue), IndexOrderNone)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }

  test("should not accept if non-cached property from ordered relationship index plan is returned") {
    val plan = new LogicalPlanBuilder(true)
      .produceResults("a")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(
      Seq(s"${plan.leftmostLeaf.toString} does not cache a.prop, but the entity is returned in ProduceResult.")
    )
  }

  test("should accept if non-cached property from ordered relationship index plan is used in UpdatePlan") {
    // SET, potentially nested in MERGE or FOREACH, is allowed to skip using a caching property.
    // In these cases we anyway will plan a Sort, so we don't run the risk of returning out-of-order results.
    val plan = new LogicalPlanBuilder(false)
      .setRelationshipProperty("a", "prop", "a.prop + 1")
      .relationshipIndexOperator("(x)-[a:A(prop)]->(y)", Map("prop" -> DoNotGetValue), IndexOrderAscending)
      .build()

    val result = OrderedIndexPlansUseCachedProperties(plan)(CancellationChecker.neverCancelled())
    result should be(empty)
  }
}
