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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NamedPathProjectionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def plannerConfig(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setRelationshipCardinality("(:X)-[]->()", 100)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

  // p = (a:X)-[r]->(b)
  private val pathExpr: PathExpression =
    PathExpression(
      NodePathStep(
        v"a",
        SingleRelationshipPathStep(v"r", SemanticDirection.OUTGOING, Some(v"b"), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

  test("should build plans containing outgoing path projections") {
    val cfg = plannerConfig()
    val plan = cfg.plan("MATCH p = (a:X)-[r]->(b) RETURN p").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection(Map("p" -> pathExpr))
      .expandAll("(a)-[r]->(b)")
      .nodeByLabelScan("a", "X")
      .build()
  }

  test("should build plans containing path projections and path selections") {
    val cfg = plannerConfig()
    val plan = cfg.plan("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a RETURN b").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filterExpression(
        equals(
          function("head", function("nodes", pathExpr)),
          v"a"
        )
      )
      .expandAll("(a)-[r]->(b)")
      .nodeByLabelScan("a", "X")
      .build()
  }

  test("should build plans containing multiple path projections and path selections") {
    val cfg = plannerConfig()
    val plan =
      cfg.plan("MATCH p = (a:X)-[r]->(b) WHERE head(nodes(p)) = a AND length(p) > 10 RETURN b").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filterExpression(
        equals(
          function("head", function("nodes", pathExpr)),
          v"a"
        ),
        greaterThan(
          function("length", pathExpr),
          literalInt(10)
        )
      )
      .expandAll("(a)-[r]->(b)")
      .nodeByLabelScan("a", "X", IndexOrderNone)
      .build()
  }
}
