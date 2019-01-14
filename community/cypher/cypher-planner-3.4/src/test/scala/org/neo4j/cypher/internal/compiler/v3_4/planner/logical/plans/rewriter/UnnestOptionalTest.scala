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
import org.neo4j.cypher.internal.ir.v3_4.VarPatternLength
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class UnnestOptionalTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should rewrite Apply/Optional/Expand to OptionalExpand when lhs of expand is single row") {
    val argument: LogicalPlan = Argument(Set("a"))
    val rhs:LogicalPlan =
      Optional(
        Expand(argument, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"
        ))
    val lhs = newMockedLogicalPlan("a")
    val input = Apply(lhs, rhs)

    input.endoRewrite(unnestOptional) should equal(
      OptionalExpand(lhs, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll, Seq.empty))
  }

  test("should not rewrite Apply/Optional/Selection/Expand to OptionalExpand when expansion is variable length") {
    val argument: LogicalPlan = Argument(Set("a"))
    val expand = VarExpand(argument, "a", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq.empty, "b", "r", VarPatternLength(1, None), ExpandAll, "tempNode", "tempEdge", TRUE, TRUE, Seq.empty)
    val predicate: Equals = Equals(Property(varFor("b"), PropertyKeyName("prop")(pos))(pos), SignedDecimalIntegerLiteral("1")(pos))(pos)
    val selection = Selection(Seq(predicate), expand)
    val rhs: LogicalPlan = Optional(selection)
    val lhs = newMockedLogicalPlan("a")
    val input = Apply(lhs, rhs)

    input.endoRewrite(unnestOptional) should equal(input)
  }

  test("should not rewrite plans containing merges") {
    val argument: LogicalPlan = Argument(Set("a"))
    val rhs:LogicalPlan =
      Optional(
        Expand(argument, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"
        ))
    val lhs = newMockedLogicalPlan("a")
    val apply = Apply(lhs, rhs)
    val mergeRel = MergeCreateRelationship(Argument(), "r", "a", RelTypeName("T")(pos), "b",
                                           None)

    val input = AntiConditionalApply(apply, mergeRel, Seq.empty)

    input.endoRewrite(unnestOptional) should equal(input)
  }
}
