/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.csv.reader.Configuration
import org.neo4j.csv.reader.Configuration.DEFAULT_BUFFER_SIZE_4MB
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.ir.v3_4.NoHeaders
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.v3_4.expressions.StringLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans._

class cleanUpEagerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should concatenate two eagers after eachother") {
    val leaf = newMockedLogicalPlan()
    val eager1 = Eager(leaf)
    val eager2 = Eager(eager1)
    val topPlan = Projection(eager2, Map.empty)

    rewrite(topPlan) should equal(Projection(Eager(leaf), Map.empty))
  }

  test("should not move eager below unwind") {
    val leaf = newMockedLogicalPlan()
    val eager = Eager(leaf)
    val unwind = UnwindCollection(eager, "i", null)
    val topPlan = Projection(unwind, Map.empty)

    rewrite(topPlan) should equal(topPlan)
  }

  test("should move eager on top of unwind to below it") {
    val leaf = newMockedLogicalPlan()
    val unwind = UnwindCollection(leaf, "i", null)
    val eager = Eager(unwind)
    val topPlan = Projection(eager, Map.empty)

    rewrite(topPlan) should equal(Projection(UnwindCollection(Eager(leaf), "i", null), Map.empty))
  }

  test("should move eager on top of unwind to below it repeatedly") {
    val leaf = newMockedLogicalPlan()
    val unwind1 = UnwindCollection(leaf, "i", null)
    val eager1 = Eager(unwind1)
    val unwind2 = UnwindCollection(eager1, "i", null)
    val eager2 = Eager(unwind2)
    val unwind3 = UnwindCollection(eager2, "i", null)
    val eager3 = Eager(unwind3)
    val topPlan = Projection(eager3, Map.empty)

    rewrite(topPlan) should equal(
      Projection(
        UnwindCollection(
          UnwindCollection(
            UnwindCollection(Eager(leaf), "i", null),
            "i", null),
          "i", null),
        Map.empty))
  }

  test("should move eager on top of load csv to below it") {
    val leaf = newMockedLogicalPlan()
    val url = StringLiteral("file:///tmp/foo.csv")(pos)
    val loadCSV = LoadCSV(leaf, url, "a", NoHeaders, None, legacyCsvQuoteEscaping = false, DEFAULT_BUFFER_SIZE_4MB)
    val eager = Eager(loadCSV)
    val topPlan = Projection(eager, Map.empty)

    rewrite(topPlan) should equal(Projection(LoadCSV(Eager(leaf), url, "a", NoHeaders, None, false, DEFAULT_BUFFER_SIZE_4MB), Map.empty))
  }

  test("should move eager on top of limit to below it") {
    val leaf = newMockedLogicalPlan()

    rewrite(
      Projection(
        Limit(
          Eager(leaf), literalInt(12), DoNotIncludeTies), Map.empty)) should equal(
      Projection(
        Eager(
          Limit(leaf, literalInt(12), DoNotIncludeTies)), Map.empty))
  }

  test("should not rewrite plan with eager below load csv") {
    val leaf = newMockedLogicalPlan()
    val eager = Eager(leaf)
    val loadCSV = LoadCSV(eager, StringLiteral("file:///tmp/foo.csv")(pos), "a", NoHeaders, None, false, DEFAULT_BUFFER_SIZE_4MB)
    val topPlan = Projection(loadCSV, Map.empty)

    rewrite(topPlan) should equal(topPlan)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(cleanUpEager(new StubSolveds, Attributes(idGen))))(p)
}
