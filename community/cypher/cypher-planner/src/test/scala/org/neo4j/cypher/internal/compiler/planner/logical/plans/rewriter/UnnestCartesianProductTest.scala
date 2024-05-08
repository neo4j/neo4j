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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnnestCartesianProductTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should unnest cartesian product with a single Argument on the lhs") {
    val input = new LogicalPlanBuilder()
      .produceResults("x")
      .cartesianProduct()
      .|.fakeLeafPlan("x")
      .argument("x")
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("x")
      .fakeLeafPlan("x")
      .build())
  }

  test("should unnest cartesian product with a single Argument on the rhs") {
    val input = new LogicalPlanBuilder()
      .produceResults("x")
      .cartesianProduct()
      .|.argument("x")
      .fakeLeafPlan("x")
      .build()

    rewrite(input) should equal(new LogicalPlanBuilder()
      .produceResults("x")
      .fakeLeafPlan("x")
      .build())
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint(CancellationChecker.neverCancelled())((p: LogicalPlan) => p.endoRewrite(unnestCartesianProduct))(p)
}
