/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class SubqueryExpressionPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport with BeLikeMatcher {

  test("should plan CREATE followed by container access with COUNT expression as an index") {
    val planner = plannerBuilder().setAllNodesCardinality(100).build()
    val q = "CREATE (a) RETURN [1, 2, 3][COUNT { MATCH (x) }] AS result"
    val plan = planner.plan(q).stripProduceResults

    val resultExpr = containerIndex(
      listOfInt(1, 2, 3),
      NestedPlanGetByNameExpression(
        planner.subPlanBuilder()
          .nodeCountFromCountStore("anon_0", Seq(None))
          .build(),
        "anon_0",
        "COUNT { MATCH (x) }"
      )(pos)
    )

    plan shouldEqual planner.subPlanBuilder()
      .projection(project = Map("result" -> resultExpr), discard = Set("a"))
      .eager(ListSet(EagernessReason.Unknown))
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("should plan CREATE followed by container access with GetDegree as an index") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]->()", 50)
      .build()

    val q = "CREATE (a) RETURN [1, 2, 3][COUNT { MATCH (a)-->() }] AS result"
    val plan = planner.plan(q).stripProduceResults

    val resultExpr = containerIndex(
      listOfInt(1, 2, 3),
      getDegree(varFor("a"), SemanticDirection.OUTGOING)
    )

    plan shouldEqual planner.subPlanBuilder()
      .projection(project = Map("result" -> resultExpr), discard = Set("a"))
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("should plan multiple EXISTS with inequality predicate") {
    val planner = plannerBuilder().setAllNodesCardinality(100).build()
    val q = "RETURN EXISTS { (a) } <> EXISTS { (b) } <> EXISTS { (c) } AS result"
    val plan = planner.plan(q).stripProduceResults
    plan should beLike {
      case Projection(_, _, projectExpressions) =>
        projectExpressions shouldBe Map(
          "result" ->
            ands(
              not(equals(varFor("anon_2"), varFor("anon_3"))),
              not(equals(varFor("anon_4"), varFor("anon_5")))
            )
        )
    }
  }
}
