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

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.ir.v3_4.VarPatternLength
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._

class UnnestApplyTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should unnest apply with a single Argument on the lhs") {
    val rhs = newMockedLogicalPlan()
    val argument = Argument()
    val input = Apply(argument, rhs)

    rewrite(input) should equal(rhs)
  }

  test("should unnest apply with a single Argument on the rhs") {
    val lhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)
    val input = Apply(lhs, argument)

    rewrite(input) should equal(lhs)
  }

  test("should unnest also when deeper in the structure") {
    val lhs = newMockedLogicalPlan()
    val argument = Argument(Set.empty)
    val apply = Apply(lhs, argument)
    val optional = Optional(apply)

    rewrite(optional) should equal(Optional(lhs))
  }

  test("should unnest one level deeper") {
    /*
           Apply
         LHS  OuterJoin
              Arg   RHS
     */
    val argPlan = Argument(Set("a"))
    val lhs = newMockedLogicalPlan("a")
    val rhs = newMockedLogicalPlan("a")

    val input =
      Apply(lhs,
        LeftOuterHashJoin(Set("a"),
          argPlan,
          rhs
        )
      )

    rewrite(input) should equal(
      LeftOuterHashJoin(Set("a"), lhs, rhs)
    )
  }

  test("should unnest optional expand") {
    /*
           Apply
         LHS  OptionalExpand
                  Arg
     */
    val argPlan = Argument(Set("a"))
    val lhs = newMockedLogicalPlan("a")

    val input =
      Apply(lhs,
        OptionalExpand(argPlan, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
      )

    rewrite(input) should equal(
      OptionalExpand(lhs, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
    )
  }

  test("should not cross OPTIONAL boundaries") {
    val argPlan = Argument(Set("a"))
    val lhs = newMockedLogicalPlan("a")
    val rhs = Selection(Seq(propEquality("a", "prop", 42)), argPlan)
    val optional = Optional(rhs)

    val input = Apply(lhs, optional)

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
    val arg1 = Argument(Set("a"))
    val arg2 = Argument(Set("a"))
    val expand = Expand(arg2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
    val apply2 = Apply(arg1, expand)
    val apply = Apply(lhs, apply2)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Expand(lhs, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll))
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
    val filter = Selection(predicates, lhs2)
    val apply2 = Apply(filter, rhs)
    val apply1 = Apply(lhs1, apply2)

    // When
    val result = rewrite(apply1)

    // Then
    val filterNew = Selection(predicates, lhs1)
    val apply2New = Apply(lhs2, rhs)
    val apply1New = Apply(filterNew, apply2New)

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
    val expand = Expand(lhs2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
    val predicates = Seq(propEquality("r", "prop", 42))
    val filter = Selection(predicates, expand)
    val apply2 = Apply(filter, rhs)
    val apply1 = Apply(lhs1, apply2)

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
    val arg = Argument(Set("a"))
    val expand = VarExpand(arg, "a", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq.empty, "b", "r", VarPatternLength(1, None), ExpandAll, "tempNode", "tempEdge", TRUE, TRUE, Seq.empty)
    val apply = Apply(lhs, expand)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(VarExpand(lhs, "a", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq.empty, "b", "r", VarPatternLength(1, None), ExpandAll, "tempNode", "tempEdge", TRUE, TRUE, Seq.empty))
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
    val arg1 = Argument(Set("a"))
    val arg2 = Argument(Set("a"))
    val expand = Expand(arg2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
    val optional = Optional(expand)
    val apply2 = Apply(arg1, optional)
    val apply = Apply(lhs, apply2)

    // When
    val result = rewrite(apply)

    // Then
    result should equal(Apply(
      lhs,
      Optional(
        Expand(arg2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll))
    ))
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
    val arg1 = Argument(Set("a"))
    val arg2 = Argument(Set("a"))
    val expand = Expand(arg2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
    val optional = Optional(expand)
    val apply2 = Apply(arg1, optional)
    val aca = AntiConditionalApply(lhs, apply2, Seq("a"))

    // When
    val result = rewrite(aca)

    // Then
    result should equal(AntiConditionalApply(
      lhs,
      Optional(
        Expand(arg2, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)),
      Seq("a")
    ))
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    fixedPoint((p: LogicalPlan) => p.endoRewrite(unnestApply(new StubSolveds, Attributes(idGen))))(p)
}
