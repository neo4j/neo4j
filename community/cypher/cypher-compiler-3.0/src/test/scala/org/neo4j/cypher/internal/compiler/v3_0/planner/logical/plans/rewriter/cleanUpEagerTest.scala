/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class cleanUpEagerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should concatenate two eagers after eachother") {
    val leaf = newMockedLogicalPlan()
    val eager1 = Eager(leaf)(solved)
    val eager2 = Eager(eager1)(solved)
    val topPlan = Projection(eager2, Map.empty)(solved)

    rewrite(topPlan) should equal(Projection(Eager(leaf)(solved), Map.empty)(solved))
  }

  test("should not move eager below unwind") {
    val leaf = newMockedLogicalPlan()
    val eager = Eager(leaf)(solved)
    val unwind = UnwindCollection(eager, IdName("i"), null)(solved)
    val topPlan = Projection(unwind, Map.empty)(solved)

    rewrite(topPlan) should equal(topPlan)
  }

  test("should move eager on top of unwind to below it") {
    val leaf = newMockedLogicalPlan()
    val unwind = UnwindCollection(leaf, IdName("i"), null)(solved)
    val eager = Eager(unwind)(solved)
    val topPlan = Projection(eager, Map.empty)(solved)

    rewrite(topPlan) should equal(Projection(UnwindCollection(Eager(leaf)(solved), IdName("i"), null)(solved), Map.empty)(solved))
  }

  test("should move eager on top of unwind to below it repeatedly") {
    val leaf = newMockedLogicalPlan()
    val unwind1 = UnwindCollection(leaf, IdName("i"), null)(solved)
    val eager1 = Eager(unwind1)(solved)
    val unwind2 = UnwindCollection(eager1, IdName("i"), null)(solved)
    val eager2 = Eager(unwind2)(solved)
    val unwind3 = UnwindCollection(eager2, IdName("i"), null)(solved)
    val eager3 = Eager(unwind3)(solved)
    val topPlan = Projection(eager3, Map.empty)(solved)

    rewrite(topPlan) should equal(
      Projection(
        UnwindCollection(
          UnwindCollection(
            UnwindCollection(Eager(leaf)(solved), IdName("i"), null)(solved),
            IdName("i"), null)(solved),
          IdName("i"), null)(solved),
        Map.empty)(solved))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(cleanUpEager))(p)
}
