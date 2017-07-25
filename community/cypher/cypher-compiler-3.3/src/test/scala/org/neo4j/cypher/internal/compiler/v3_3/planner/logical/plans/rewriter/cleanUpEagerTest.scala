/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.ast.StringLiteral
import org.neo4j.cypher.internal.frontend.v3_3.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{IdName, NoHeaders}

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

  test("should move eager on top of load csv to below it") {
    val leaf = newMockedLogicalPlan()
    val url = StringLiteral("file:///tmp/foo.csv")(pos)
    val loadCSV = LoadCSV(leaf, url, IdName("a"), NoHeaders, None, false)(solved)
    val eager = Eager(loadCSV)(solved)
    val topPlan = Projection(eager, Map.empty)(solved)

    rewrite(topPlan) should equal(Projection(LoadCSV(Eager(leaf)(solved), url, IdName("a"), NoHeaders, None, false)(solved), Map.empty)(solved))
  }

  test("should move eager on top of limit to below it") {
    val leaf = newMockedLogicalPlan()

    rewrite(
      Projection(
        Limit(
          Eager(leaf)(solved), literalInt(12), DoNotIncludeTies)(solved), Map.empty)(solved)) should equal(
      Projection(
        Eager(
          Limit(leaf, literalInt(12), DoNotIncludeTies)(solved))(solved), Map.empty)(solved))
  }

  test("should not rewrite plan with eager below load csv") {
    val leaf = newMockedLogicalPlan()
    val eager = Eager(leaf)(solved)
    val loadCSV = LoadCSV(eager, StringLiteral("file:///tmp/foo.csv")(pos), IdName("a"), NoHeaders, None, false)(solved)
    val topPlan = Projection(loadCSV, Map.empty)(solved)

    rewrite(topPlan) should equal(topPlan)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(cleanUpEager))(p)
}
