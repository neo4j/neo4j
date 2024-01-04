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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnnestOptionalTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should rewrite Apply/Optional/Expand to OptionalExpand when lhs of expand is single row") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val rhs: LogicalPlan =
      Optional(
        Expand(argument, v"a", SemanticDirection.OUTGOING, Seq.empty, v"b", v"r")
      )
    val lhs = newMockedLogicalPlan("a")
    val input = Apply(lhs, rhs)

    input.endoRewrite(unnestOptional) should equal(
      OptionalExpand(lhs, v"a", SemanticDirection.OUTGOING, Seq.empty, v"b", v"r", ExpandAll, None)
    )
  }

  test("should not rewrite Apply/Optional/Selection/Expand to OptionalExpand when expansion is variable length") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val expand = VarExpand(
      argument,
      v"a",
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      Seq.empty,
      v"b",
      v"r",
      VarPatternLength(1, None),
      ExpandAll
    )
    val predicate = propEquality("b", "prop", 1)
    val selection = Selection(Seq(predicate), expand)
    val rhs: LogicalPlan = Optional(selection)
    val lhs = newMockedLogicalPlan("a")
    val input = Apply(lhs, rhs)

    input.endoRewrite(unnestOptional) should equal(input)
  }

  test("should not rewrite plans containing merges") {
    val argument: LogicalPlan = Argument(Set(v"a"))
    val rhs: LogicalPlan =
      Optional(
        Expand(argument, v"a", SemanticDirection.OUTGOING, Seq.empty, v"b", v"r")
      )
    val lhs = newMockedLogicalPlan("a")
    val apply = Apply(lhs, rhs)
    val mergeRel = Merge(
      Argument(),
      Seq.empty,
      Seq(createRelationship("r", "a", "T", "b", SemanticDirection.OUTGOING)),
      Seq.empty,
      Seq.empty,
      Set.empty
    )

    val input = AntiConditionalApply(apply, mergeRel, Seq.empty)

    input.endoRewrite(unnestOptional) should equal(input)
  }
}
