/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class FuseSelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  test("merges two selections into one") {
    val p1 = propEquality("a", "foo", 12)
    val p2 = propEquality("a", "bar", 33)
    val lhs = Argument(Set(IdName("a")))(solved)()

    Selection(Seq(p1),
      Selection(Seq(p2), lhs)(solved))(solved).
      endoRewrite(fuseSelections) should equal(
      Selection(Seq(p1, p2), lhs)(solved)
    )
  }

  test("merges three selections into one") {
    val p1 = propEquality("a", "foo", 12)
    val p2 = propEquality("a", "bar", 33)
    val p3 = propEquality("a", "baz", 42)
    val lhs = Argument(Set(IdName("a")))(solved)()

    Selection(Seq(p1),
      Selection(Seq(p2),
        Selection(Seq(p3), lhs)(solved))(solved))(solved).
      endoRewrite(fuseSelections) should equal(
      Selection(Seq(p1, p2, p3), lhs)(solved)
    )
  }
}
