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
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlanRewritingPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setRelationshipCardinality("()-[]->()", 100)
    .build()

  test("should use GetDegree to compute the degree of a node") {
    val plan = planner.plan("MATCH (n) RETURN size((n)-->()) AS deg").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("deg" -> getDegree(varFor("n"), OUTGOING)))
      .allNodeScan("n")
      .build()
  }

  test("should insert limit for nested plan expression inside isEmpty") {
    val q =
      """
        |MATCH p = (n)
        |RETURN all(node IN nodes(p) WHERE isEmpty([(n)-[r]->(b) WHERE n.prop > 5 | b.age])) AS age
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .limit(1)
      .expandAll("(n)-[r]->(b)")
      .filter("n.prop > 5")
      .argument("n")
      .build()

    val expected =
      planner.subPlanBuilder()
        .projection(
          Map("age" ->
            AllIterablePredicate(
              FilterScope(
                varFor("node"),
                Some(IsEmpty(
                  NestedPlanCollectExpression(
                    expectedNestedPlan,
                    Property(varFor("b"),PropertyKeyName("age")(pos))(pos),
                    s"[(n)-[r]->(b) WHERE n.prop > 5 | b.age]"
                  )(pos)
                )(pos))
              )(pos),
              nodes(PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(pos))
            )(pos))
        )
        .allNodeScan("n")
        .build()
    plan should equal(expected)
  }

  test("should insert limit for nested plan expression inside Size(...) = 0") {
    val q =
      """
        |MATCH p = (n)
        |RETURN all(node IN nodes(p) WHERE size([(n)-[r]->(b) WHERE n.prop > 5 | b.age]) = 0) AS age
      """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .limit(1)
      .expandAll("(n)-[r]->(b)")
      .filter("n.prop > 5")
      .argument("n")
      .build()

    val expected =
      planner.subPlanBuilder()
        .projection(
          Map("age" ->
            AllIterablePredicate(
              FilterScope(
                varFor("node"),
                Some(Equals(
                  Size(
                    NestedPlanCollectExpression(
                      expectedNestedPlan,
                      Property(varFor("b"), PropertyKeyName("age")(pos))(pos),
                      s"[(n)-[r]->(b) WHERE n.prop > 5 | b.age]"
                    )(pos)
                  )(pos),
                  SignedDecimalIntegerLiteral("0")(pos)
                )(pos))
              )(pos),
              nodes(PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(pos))
            )(pos))
        )
        .allNodeScan("n")
        .build()
    plan should equal(expected)
  }

  test("should insert limit for nested plan expression inside Size(...) > 0") {
    val q =
      """
        |MATCH p = (n)
        |RETURN all(node IN nodes(p) WHERE size([(n)-[r]->(b) WHERE n.prop > 5 | b.age]) > 0) AS age
    """.stripMargin

    val plan = planner.plan(q).stripProduceResults

    val expectedNestedPlan = planner.subPlanBuilder()
      .limit(1)
      .expandAll("(n)-[r]->(b)")
      .filter("n.prop > 5")
      .argument("n")
      .build()

    val expected =
      planner.subPlanBuilder()
        .projection(
          Map("age" ->
            AllIterablePredicate(
              FilterScope(
                varFor("node"),
                Some(GreaterThan(
                  Size(
                    NestedPlanCollectExpression(
                      expectedNestedPlan,
                      Property(varFor("b"), PropertyKeyName("age")(pos))(pos),
                      s"[(n)-[r]->(b) WHERE n.prop > 5 | b.age]"
                    )(pos)
                  )(pos),
                SignedDecimalIntegerLiteral("0")(pos)
                )(pos))
              )(pos),
              nodes(PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(pos))
            )(pos))
        )
        .allNodeScan("n")
        .build()
    plan should equal(expected)
  }
}
