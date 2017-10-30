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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.planner.{FakePlan, LogicalPlanningTestSupport}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{DropResult, LogicalPlan, Selection}

class SimplifySelectionsTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should rewrite Selection(false, source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(False()(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should rewrite Selection(Seq(false, false, false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(False()(pos), False()(pos), False()(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult(source)(solved))
  }

  test("should rewrite Selection(Or(false, false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(Or(False()(pos), False()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should not rewrite Selection(Or(false, true), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(Or(False()(pos), True()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(selection)
  }

  test("should not rewrite Selection(Or(true, false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(Or(True()(pos), False()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(selection)
  }

  test("should not rewrite Selection(Or(true, true), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(Or(True()(pos), True()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(selection)
  }

  test("should rewrite Selection(And(false, false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(And(False()(pos), False()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should rewrite Selection(And(false, true), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(And(False()(pos), True()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should rewrite Selection(And(true, false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(And(True()(pos), False()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should rewrite Selection(And(true, or(false,false), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(And(True()(pos), Or(False()(pos), False()(pos))(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(
      DropResult( source)(solved))
  }

  test("should not rewrite Selection(And(true, true), source) to DropResult(source)") {
    val source: LogicalPlan = FakePlan(Set.empty)(solved)
    val selection = Selection(Seq(And(True()(pos), True()(pos))(pos)), source)(solved)

    selection.endoRewrite(simplifySelections) should equal(selection)
  }

}
