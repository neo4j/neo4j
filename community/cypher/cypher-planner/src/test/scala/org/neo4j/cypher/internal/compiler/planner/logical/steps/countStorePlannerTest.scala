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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphProducer
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

class countStorePlannerTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer {

  override val semanticFeatures: List[SemanticFeature] = List(SemanticFeature.GpmShortestPath)

  test("should ignore tail") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val pq = producePlannerQuery("MATCH (n)", "n")

    countStorePlanner(pq.withTail(null), context) should beCountPlanFor("n")
  }

  test("should plan a count for node count no labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n)", "n")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("n")
  }

  test("should plan a count for node count with * and no labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n) RETURN count(*)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should beCountPlanFor("*")
  }

  test("should not plan a count if no match provided") {
    val query = "RETURN count(*)"
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for simple cases even if nodes are unnamed") {
    val query = "MATCH () RETURN count(*)"
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should beCountPlanFor("*")
  }

  test("should plan a count for cartesian node count no labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n), (m)", "n")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("n")
  }

  test("should plan a count for cartesian node count with * and no labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n), (m) RETURN count(*)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should beCountPlanFor("*")
  }

  test("should not plan a count for node count when there is a predicate on the node") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE n.prop", "n")

    // Then
    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for node count with * and when there is a predicate on the node") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n) WHERE n.prop RETURN count(*)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for node count when there is a predicate on something else") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE 1 = 2", "n")

    // Then
    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on the relationship") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE r.prop", "r")

    // Then
    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on something else") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE 1 = 2", "r")
    // Then
    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for node count with a label") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label)", "n")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("n")
  }

  test("should not plan a count for node count with more than one label") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label1:Label2)", "n")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for rel count with no direction") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-()", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for rel count with no type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]->()", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and no type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r]->()", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and no type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()<-[r]-(:Label1)", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count with both ended labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r]-(:Label2)", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for rel endpoint count with both ended labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (u:Label1)<-[]-(:Label2)", "u")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count for rel count with type but no direction") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]-()", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for rel count with rel type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]->()", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and rel type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r:X]->()", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and rel type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()<-[r:X]-(:Label1)", "r")
    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should plan a count for relationship count with lhs label and rel type when variable is on lhs node") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (u:User)-[:KNOWS]->() RETURN count(u)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)
    // Then
    countStorePlanner(plannerQuery, context) should beCountPlanFor("u")
  }

  test("should plan a count for relationship count with lhs label and rel type when variable is on rhs node") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (:User)-[:KNOWS]->(x) RETURN count(x)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)
    // Then
    countStorePlanner(plannerQuery, context) should beCountPlanFor("x")
  }

  test("should plan a count for relationship count with rhs label and rel type when variable is on rhs node") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH ()-[:KNOWS]->(u:User) RETURN count(u)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)
    // Then
    countStorePlanner(plannerQuery, context) should beCountPlanFor("u")
  }

  test("should plan a count for relationship count with rhs label and rel type when variable is on lhs node") {
    // When
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (x)-[:KNOWS]->(:User) RETURN count(x)"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)
    // Then
    countStorePlanner(plannerQuery, context) should beCountPlanFor("x")
  }

  test("should not plan a count for rel count with both ended labels and rel type") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r:X]-(:Label2)", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count when node variable is a dependency") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = {
      val plannerQuery = producePlannerQuery("MATCH (n:Label)", "n")
      val qg = plannerQuery.queryGraph
      plannerQuery.withQueryGraph(qg.addArgumentId("n"))
    }

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a count when rel variable is a dependency") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = {
      val plannerQuery = producePlannerQuery("MATCH (n)-[r:REL]->(m)", "r")
      val qg = plannerQuery.queryGraph
      plannerQuery.withQueryGraph(qg.addArgumentId("r"))
    }

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a rel count when start node has multiple labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label1:Label2)-[r:REL]->()", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a rel count when end node has multiple labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r:REL]->(n:Label1:Label2)", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a rel count when start and end nodes have multiple labels") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label1:Label2)-[r:REL]->(m:Label1:Label2)", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count store operator also when there is a selection on the aggregation") {
    val queryText = "MATCH (n) WITH count(n) AS nodeCount WHERE nodeCount > 0 RETURN nodeCount"
    val (plannerQuery, _) = producePlannerQueryForPattern(queryText, appendReturn = false)

    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val maybePlan = countStorePlanner(plannerQuery, context)

    maybePlan should equal(Some(
      new LogicalPlanBuilder(wholePlan = false)
        .filter("nodeCount > 0")
        .nodeCountFromCountStore("nodeCount", Seq(None))
        .build()
    ))
  }

  test("should not plan a node count store operator when horizon has skip") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n) RETURN count(n) SKIP 1"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a relationship count store operator when horizon has skip") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n)-[r]->(m) RETURN count(r) SKIP 1"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a node count store operator when horizon has limit") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n) RETURN count(n) LIMIT 0"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should not plan a relationship count store operator when horizon has limit") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val query = "MATCH (n)-[r]->(m) RETURN count(r) LIMIT 0"
    val (plannerQuery, _) = producePlannerQueryForPattern(query, appendReturn = false)

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  test("should plan a count for simple node count under ALL SHORTEST") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ALL SHORTEST (n:Label)", "n")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("n")
  }

  test("should plan a count for simple rel count under ALL SHORTEST") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ALL SHORTEST (:Label1)-[r:X]->()", "r")

    countStorePlanner(plannerQuery, context) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count under ANY SHORTEST") {
    val context = newMockedLogicalPlanningContextWithFakeAttributes(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ANY SHORTEST (:Label1)-[r]->()", "r")

    countStorePlanner(plannerQuery, context) should notBeCountPlan
  }

  private def producePlannerQuery(query: String, variable: String) = {
    val (pq, _) = producePlannerQueryForPattern(query)
    pq.withHorizon(AggregatingQueryProjection(
      aggregationExpressions = Map(v"count($variable)" -> count(varFor(variable)))
    ))
  }

  case class IsCountPlan(variable: String, noneExpected: Boolean) extends Matcher[Option[LogicalPlan]] {

    override def apply(plan: Option[LogicalPlan]): MatchResult = {
      if (noneExpected) {
        MatchResult(plan.isEmpty, s"Got a plan when none expected: ${plan.getOrElse("")}", "")
      } else {
        MatchResult(
          plan match {
            case Some(NodeCountFromCountStore(LogicalVariable(countId), _, _)) if countId == s"count($variable)" =>
              true
            case Some(RelationshipCountFromCountStore(LogicalVariable(countId), _, _, _, _))
              if countId == s"count($variable)" =>
              true
            case _ =>
              false
          },
          "No count store plan produced",
          ""
        )
      }
    }
  }

  private def beCountPlanFor(variable: String) = IsCountPlan(variable, noneExpected = false)
  private def notBeCountPlan = IsCountPlan("", noneExpected = true)

}
