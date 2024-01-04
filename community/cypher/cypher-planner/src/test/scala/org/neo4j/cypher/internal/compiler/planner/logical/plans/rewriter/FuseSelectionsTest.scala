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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FuseSelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("merges two selections into one") {
    val p1 = propEquality("a", "foo", 12)
    val p2 = propEquality("a", "bar", 33)
    val lhs = Argument(Set(v"a"))

    Selection(Seq(p1), Selection(Seq(p2), lhs)).endoRewrite(fuseSelections) should equal(
      Selection(Seq(p1, p2), lhs)
    )
  }

  test("merges three selections into one") {
    val p1 = propEquality("a", "foo", 12)
    val p2 = propEquality("a", "bar", 33)
    val p3 = propEquality("a", "baz", 42)
    val lhs = Argument(Set(v"a"))

    Selection(Seq(p1), Selection(Seq(p2), Selection(Seq(p3), lhs))).endoRewrite(fuseSelections) should equal(
      Selection(Seq(p1, p2, p3), lhs)
    )
  }
}
