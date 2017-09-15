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

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticDirection
import org.neo4j.cypher.internal.ir.v3_4.{IdName, VarPatternLength}
import org.neo4j.cypher.internal.v3_4.logical.plans._

class UnnestApplyTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should unnest apply with a single SingleRow on the lhs") {
    val rhs = newMockedLogicalPlan()
    val singleRow = SingleRow()(solved)
    val input = Apply(singleRow, rhs)(solved)

    rewrite(input) should equal(rhs)
  }

  test("should unnest apply with a single SingleRow on the rhs") {
    val lhs = newMockedLogicalPlan()
    val argument = SingleRow()(solved)
    val input = Apply(lhs, argument)(solved)

    rewrite(input) should equal(lhs)
  }

  test("should unnest apply with a single Argument on the rhs") {
    val lhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)(solved)()
    val input = Apply(lhs, argument)(solved)

    rewrite(input) should equal(lhs)
  }

  test("should unnest apply with a single Argument on the lhs") {
    val rhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)(solved)()
    val input = Apply(argument, rhs)(solved)

    rewrite(input) should equal(rhs)
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
    val lhs = newMockedLogicalPlan("a")
    val arg1 = Argument(Set(IdName("a")))(solved)()
    val arg2 = Argument(Set(IdName("a")))(solved)()
    val expand = Expand(arg2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val apply2 = Apply(arg1, expand)(solved)
    val apply = Apply(lhs, apply2)(solved)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Expand(lhs, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))
  }

  test("apply on apply should be extracted nicely 2") {
    /*  Moves filtering on a LHS of inner Apply to the LHS of the outer

                            Apply1                      Apply1
                         LHS1     Apply2       =>  Filter    Apply2
                              Filter   RHS        LHS1     LHS2   RHS
                           LHS2
     */

    // Given
    val lhs1 = newMockedLogicalPlan("a")
    val lhs2 = newMockedLogicalPlan("a")
    val rhs = newMockedLogicalPlan("a")
    val predicates = Seq(propEquality("a", "prop", 42))
    val filter = Selection(predicates, lhs2)(solved)
    val apply2 = Apply(filter, rhs)(solved)
    val apply1 = Apply(lhs1, apply2)(solved)

    // When
    val result = rewrite(apply1)

    // Then
    val filterNew = Selection(predicates, lhs1)(solved)
    val apply2New = Apply(lhs2, rhs)(solved)
    val apply1New = Apply(filterNew, apply2New)(solved)

    result should equal(apply1New)
  }

  test("apply on apply should be left unchanged") {
    /*  Does not moves filtering on a LHS of inner Apply to the LHS of the outer if Selection has a dependency on a
    variable introduced in the source operator

                            Apply1
                         LHS1     Apply2       =>  remains unchanged
                              Filter   RHS
                           Expand
                           LHS2
     */

    // Given
    val lhs1 = newMockedLogicalPlan("a")
    val lhs2 = newMockedLogicalPlan("a")
    val rhs = newMockedLogicalPlan("a")
    val expand = Expand(lhs2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val predicates = Seq(propEquality("r", "prop", 42))
    val filter = Selection(predicates, expand)(solved)
    val apply2 = Apply(filter, rhs)(solved)
    val apply1 = Apply(lhs1, apply2)(solved)

    // When
    val result = rewrite(apply1)

    // Then
    result should equal(apply1)
  }

  test("unnesting varlength expands should work well") {
    /*
                            Apply
                         LHS   VarExpand
                                     Arg
     */

    // Given
    val lhs = newMockedLogicalPlan("a")
    val arg = Argument(Set(IdName("a")))(solved)()
    val expand = VarExpand(arg, IdName("a"), SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), VarPatternLength(1, None), ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val apply = Apply(lhs, expand)(solved)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(VarExpand(lhs, IdName("a"), SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), VarPatternLength(1, None), ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved))
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
    val lhs = newMockedLogicalPlan("a")
    val arg1 = Argument(Set(IdName("a")))(solved)()
    val arg2 = Argument(Set(IdName("a")))(solved)()
    val expand = Expand(arg2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val optional = Optional(expand)(solved)
    val apply2 = Apply(arg1, optional)(solved)
    val apply = Apply(lhs, apply2)(solved)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Apply(
      lhs,
      Optional(
        Expand(arg2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))(solved)
    )(solved))
  }

  test("AntiConditionalApply on apply on optional should be OK") {
    /*
                            ACA                  ACA
                         LHS   Apply       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    // Given
    val lhs = newMockedLogicalPlan("a")
    val arg1 = Argument(Set(IdName("a")))(solved)()
    val arg2 = Argument(Set(IdName("a")))(solved)()
    val expand = Expand(arg2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved)
    val optional = Optional(expand)(solved)
    val apply2 = Apply(arg1, optional)(solved)
    val aca = AntiConditionalApply(lhs, apply2, Seq(IdName("a")))(solved)

    // When
    val result = rewrite(aca)

    // Then
    result should equal(AntiConditionalApply(
      lhs,
      Optional(
        Expand(arg2, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))(solved),
      Seq(IdName("a"))
    )(solved))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(unnestApply))(p)
}
