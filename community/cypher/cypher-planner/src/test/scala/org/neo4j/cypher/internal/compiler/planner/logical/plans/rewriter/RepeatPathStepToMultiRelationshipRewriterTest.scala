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
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RepeatPathStepToMultiRelationshipRewriterTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should rewrite RepeatPathStep with a single relationship into MultiRelationshipPathStep") {
    val pathStep =
      RepeatPathStep(
        List(NodeRelPair(v"b", v"r")),
        v"d",
        NilPathStep()(pos)
      )(pos)
    val rewrittenPathStep = MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("d")), NilPathStep()(pos))(pos)

    pathStep.endoRewrite(RepeatPathStepToMultiRelationshipRewriter) should equal(rewrittenPathStep)
  }

  test("should not rewrite RepeatPathStep with a multiple relationships into MultiRelationshipPathStep") {
    val pathStep =
      RepeatPathStep(
        List(NodeRelPair(v"b", v"r"), NodeRelPair(v"c", v"s")),
        v"d",
        NilPathStep()(pos)
      )(pos)

    shouldNotRewrite(pathStep)
  }

  test(
    "should rewrite RepeatPathStep with a single relationship inside PathExpression into MultiRelationshipPathStep"
  ) {
    val path = PathExpression(NodePathStep(
      v"a",
      RepeatPathStep(
        List(NodeRelPair(v"b", v"r")),
        v"d",
        NilPathStep()(pos)
      )(pos)
    )(pos))(pos)
    val rewrittenPath = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(varFor("r"), OUTGOING, Some(varFor("d")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    path.endoRewrite(RepeatPathStepToMultiRelationshipRewriter) should equal(rewrittenPath)
  }

  private def shouldNotRewrite(pathStep: RepeatPathStep): Unit = {
    pathStep.endoRewrite(RepeatPathStepToMultiRelationshipRewriter) should equal(pathStep)
  }
}
