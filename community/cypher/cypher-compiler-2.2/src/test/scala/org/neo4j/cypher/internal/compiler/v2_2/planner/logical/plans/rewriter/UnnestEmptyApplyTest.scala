/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.graphdb.Direction

class UnnestEmptyApplyTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should unnest apply with a single SingleRow on the lhs") {
    val rhs = newMockedLogicalPlan()
    val singleRow = SingleRow()
    val input = Apply(singleRow, rhs)(solved)

    rewrite(input) should equal(rhs)
  }

  test("should unnest apply with a single SingleRow on the rhs") {
    val lhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)(solved)()
    val input = Apply(lhs, argument)(solved)

    rewrite(input) should equal(lhs)
  }

  test("should unnest also when deeper in the structure") {
    val lhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)(solved)()
    val apply = Apply(lhs, argument)(solved)
    val optional = Optional(apply)(solved)

    rewrite(optional) should equal(Optional(lhs)(solved))
  }

  test("should unnest one level deeper") {
    /*
           Apply
         LHS  OuterJoin
              Arg   RHS
     */
    val argPlan = Argument(Set(IdName("a")))(solved)()
    val lhs = newMockedLogicalPlan("a")
    val rhs = newMockedLogicalPlan("a")

    val input =
      Apply(lhs,
        OuterHashJoin(Set(IdName("a")),
          argPlan,
          rhs
        )(solved)
      )(solved)

    rewrite(input) should equal(
      OuterHashJoin(Set(IdName("a")), lhs, rhs)(solved)
    )
  }

  test("should not cross OPTIONAL boundaries") {
    val argPlan = Argument(Set(IdName("a")))(solved)()
    val lhs = newMockedLogicalPlan("a")
    val rhs = Selection(Seq(propEquality("a", "prop", 42)), argPlan)(solved)
    val optional = Optional(rhs)(solved)

    val input = Apply(lhs, optional)(solved)

    rewrite(input) should equal(input)
  }

  test("apply on apply should be extracted nicely") {
    /*
                            Apply1
                         LHS   Apply2       =>  Expand
                             Arg1  Expand        LHS
                                      Arg2
     */

    // Given
    val lhs: LogicalPlan = newMockedLogicalPlan("a")
    val arg1: LogicalPlan = Argument(Set(IdName("a")))(solved)()
    val arg2: LogicalPlan = Argument(Set(IdName("a")))(solved)()
    val expand: LogicalPlan = Expand(arg2, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val apply2: LogicalPlan = Apply(arg1, expand)(solved)
    val apply: LogicalPlan = Apply(lhs, apply2)(solved)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Expand(lhs, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))
  }

  test("apply on apply on optional should be OK") {
    /*
                            Apply1                Apply
                         LHS   Apply2       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    // Given
    val lhs: LogicalPlan = newMockedLogicalPlan("a")
    val arg1: LogicalPlan = Argument(Set(IdName("a")))(solved)()
    val arg2: LogicalPlan = Argument(Set(IdName("a")))(solved)()
    val expand: LogicalPlan = Expand(arg2, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val optional: LogicalPlan = Optional(expand)(solved)
    val apply2: LogicalPlan = Apply(arg1, optional)(solved)
    val apply: LogicalPlan = Apply(lhs, apply2)(solved)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Apply(
      lhs,
      Optional(
        Expand(arg2, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))(solved)
    )(solved))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    iterateUntilConverged((p: LogicalPlan) => p.endoRewrite(unnestApply))(p)
}
