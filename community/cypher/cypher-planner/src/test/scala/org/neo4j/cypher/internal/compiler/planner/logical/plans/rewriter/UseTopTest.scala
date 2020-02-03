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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.IncludeTies
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UseTopTest extends CypherFunSuite with LogicalPlanningTestSupport {
  private val leaf = newMockedLogicalPlan()
  private val sortDescriptionX = Seq(Ascending("x"))
  private val sortDescriptionY = Seq(Ascending("y"))
  private val sort = Sort(leaf, sortDescriptionX)
  private val partialSort = PartialSort(leaf, sortDescriptionX, sortDescriptionY)
  private val lit10 = literalInt(10)

  test("should use Top when possible") {
    val limit = Limit(sort, lit10, DoNotIncludeTies)

    rewrite(limit) should equal(Top(leaf, sortDescriptionX, lit10))
  }

  test("should not use Top when including ties") {
    val original = Limit(sort, lit10, IncludeTies)

    rewrite(original) should equal(original)
  }

  test("should use PartialTop when possible") {
    val limit = Limit(partialSort, lit10, DoNotIncludeTies)

    rewrite(limit) should equal(PartialTop(leaf, sortDescriptionX, sortDescriptionY, lit10))
  }

  test("should not use PartialTop when including ties") {
    val original = Limit(partialSort, lit10, IncludeTies)

    rewrite(original) should equal(original)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(useTop))(p)
}
