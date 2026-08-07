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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedVariablesRewriterTest.beRewrittenTo
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedVariablesRewriterTest.notBeRewritten
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.RemoveUnusedVariablesRewriterTest.thePlan
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class RemoveUnusedVariablesRewriterTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("remove both start and end node") {
    thePlan(
      _.produceResults("r")
        .allRelationshipsScan("(a)-[r]->(b)")
    ) should beRewrittenTo(
      _.produceResults("r")
        .allRelationshipsScan("()-[r]->()")
    )
  }

  test("remove start node") {
    thePlan(
      _.produceResults("r", "b")
        .allRelationshipsScan("(a)-[r]->(b)")
    ) should beRewrittenTo(
      _.produceResults("r", "b")
        .allRelationshipsScan("()-[r]->(b)")
    )
  }

  test("remove end node") {
    thePlan(
      _.produceResults("r", "a")
        .allRelationshipsScan("(a)-[r]->(b)")
    ) should beRewrittenTo(
      _.produceResults("r", "a")
        .allRelationshipsScan("(a)-[r]->()")
    )
  }

  test("remove nothing") {
    thePlan(
      _.produceResults("r", "a", "b")
        .allRelationshipsScan("(a)-[r]->(b)")
    ) should notBeRewritten
  }

  test("remove relationship") {
    thePlan(
      _.produceResults("a", "b")
        .allRelationshipsScan("(a)-[r]->(b)")
    ) should beRewrittenTo(
      _.produceResults("a", "b")
        .allRelationshipsScan("(a)-[]->(b)")
    )
  }

  test("remove end-node and relationship from expand") {
    thePlan(
      _.produceResults("a")
        .expand("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a")
        .expand("(a)-[]->()")
        .allNodeScan("a")
    )
  }

  test("remove relationship from expand") {
    thePlan(
      _.produceResults("a", "b")
        .expand("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "b")
        .expand("(a)-[]->(b)")
        .allNodeScan("a")
    )
  }

  test("remove end-node from expand") {
    thePlan(
      _.produceResults("a", "r")
        .expand("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "r")
        .expand("(a)-[r]->()")
        .allNodeScan("a")
    )
  }

  test("remove end-node and relationship from optional expand") {
    thePlan(
      _.produceResults("a")
        .optionalExpandAll("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a")
        .optionalExpandAll("(a)-[]->()")
        .allNodeScan("a")
    )
  }

  test("remove relationship from optional expand") {
    thePlan(
      _.produceResults("a", "b")
        .optionalExpandAll("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "b")
        .optionalExpandAll("(a)-[]->(b)")
        .allNodeScan("a")
    )
  }

  test("remove end-node from optional expand") {
    thePlan(
      _.produceResults("a", "r")
        .optionalExpandAll("(a)-[r]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "r")
        .optionalExpandAll("(a)-[r]->()")
        .allNodeScan("a")
    )
  }

  test("remove end-node and relationship from var-expand") {
    thePlan(
      _.produceResults("a")
        .expand("(a)-[r*]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a")
        .expand("(a)-[*]->()")
        .allNodeScan("a")
    )
  }

  test("remove relationship from var-expand") {
    thePlan(
      _.produceResults("a")
        .expand("(a)-[r*..77]->(b)", ExpandInto)
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a")
        .expand("(a)-[*..77]->(b)", ExpandInto)
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
    )
  }

  test("remove end-node from var-expand") {
    thePlan(
      _.produceResults("a", "r")
        .expand("(a)-[r*]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "r")
        .expand("(a)-[r*]->()")
        .allNodeScan("a")
    )
  }

  test("don't remove anything from var-expand if variables are used") {
    thePlan(
      _.produceResults("a", "b", "r")
        .expand("(a)-[r*]->(b)")
        .allNodeScan("a")
    ) should notBeRewritten
  }

  test("remove end-node from bfs-pruning-var-expand") {
    thePlan(
      _.produceResults("a", "r")
        .bfsPruningVarExpand("(a)-[r*]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "r")
        .bfsPruningVarExpand("(a)-[r*]->()")
        .allNodeScan("a")
    )
  }

  test("don't remove anything from bfs-pruning-var-expand if used") {
    thePlan(
      _.produceResults("a", "b", "r")
        .bfsPruningVarExpand("(a)-[r*]->(b)")
        .allNodeScan("a")
    ) should notBeRewritten
  }

  test("remove end-node from pruning-var-expand") {
    thePlan(
      _.produceResults("a", "r")
        .pruningVarExpand("(a)-[r*2..7]->(b)")
        .allNodeScan("a")
    ) should beRewrittenTo(
      _.produceResults("a", "r")
        .pruningVarExpand("(a)-[r*2..7]->()")
        .allNodeScan("a")
    )
  }

  test("don't remove anything from pruning-var-expand if used") {
    thePlan(
      _.produceResults("a", "b", "r")
        .pruningVarExpand("(a)-[r*2..7]->(b)")
        .allNodeScan("a")
    ) should notBeRewritten
  }

  test("should remove unused variables in UNWIND") {
    thePlan(
      _.produceResults("a")
        .apply()
        .|.unwind("[1, 2, 3] AS b")
        .|.argument("a")
        .unwind("[1, 2, 3] AS a")
        .argument()
    ) should beRewrittenTo(
      _.produceResults("a")
        .apply()
        .|.unwind("[1, 2, 3] AS _")
        .|.argument("a")
        .unwind("[1, 2, 3] AS a")
        .argument()
    )
  }
}

object RemoveUnusedVariablesRewriterTest extends CypherPlannerTestSuite {

  case class beRewrittenTo(factory: LogicalPlanBuilder => LogicalPlanBuilder) extends Matcher[LogicalPlan] {

    override def apply(left: LogicalPlan): MatchResult = {
      val leftRewritten = rewrite(left)
      val right = thePlan(factory)
      MatchResult(leftRewritten == right, s"$leftRewritten did not equal $right", s"$leftRewritten did equal $right")
    }
  }

  case object notBeRewritten extends Matcher[LogicalPlan] {

    override def apply(left: LogicalPlan): MatchResult = {
      val leftRewritten = rewrite(left)
      MatchResult(left == leftRewritten, s"$left was rewritten to $leftRewritten", s"$left wasn't rewritten")
    }
  }

  def thePlan(factory: LogicalPlanBuilder => LogicalPlanBuilder): LogicalPlan = {
    val builder = new LogicalPlanBuilder()
    factory(builder).build()
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(RemoveUnusedVariablesRewriter)
}
