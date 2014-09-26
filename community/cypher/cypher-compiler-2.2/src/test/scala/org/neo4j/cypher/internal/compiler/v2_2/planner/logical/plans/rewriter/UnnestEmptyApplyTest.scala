/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

class UnnestEmptyApplyTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should unnest apply with a single SingleRow on the lhs") {
    val rhs = newMockedLogicalPlan()
    val singleRow = SingleRow(Set.empty)(solved)()
    val input = Apply(singleRow, rhs)(solved)

    input.endoRewrite(unnestEmptyApply) should equal(rhs)
  }

  test("should unnest apply with a single SingleRow on the rhs") {
    val lhs = newMockedLogicalPlan()
    val singleRow = SingleRow(Set.empty)(solved)()
    val input = Apply(lhs, singleRow)(solved)

    input.endoRewrite(unnestEmptyApply) should equal(lhs)
  }

  test("should unnest also when deeper in the structure") {
    val lhs = newMockedLogicalPlan()
    val singleRow = SingleRow(Set.empty)(solved)()
    val apply = Apply(lhs, singleRow)(solved)
    val optional = Optional(apply)(solved)

    optional.endoRewrite(unnestEmptyApply) should equal(Optional(lhs)(solved))
  }

  test("should not take on plans that do not match the description") {
    val lhs = newMockedLogicalPlan()
    val rhs = newMockedLogicalPlan()
    val input = Apply(lhs, rhs)(solved)

    input.endoRewrite(unnestEmptyApply) should equal(input)
  }
}
