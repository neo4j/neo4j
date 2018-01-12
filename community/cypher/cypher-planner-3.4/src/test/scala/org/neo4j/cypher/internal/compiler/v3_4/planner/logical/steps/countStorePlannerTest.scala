/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.QueryGraphProducer
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.ir.v3_4.AggregatingQueryProjection
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.expressions.{FunctionInvocation, FunctionName, Variable}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NodeCountFromCountStore, RelationshipCountFromCountStore}
import org.scalatest.matchers.{MatchResult, Matcher}

class countStorePlannerTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer with AstConstructionTestSupport {

  test("should ignore tail") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val pq = producePlannerQuery("MATCH (n)", "n", solveds, cardinalities)

    countStorePlanner(pq.withTail(null), context, solveds, cardinalities) should beCountPlanFor("n")
  }

  test("should plan a count for node count no labels") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n)", "n", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("n")
  }

  test("should plan a count for cartesian node count no labels") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n), (m)", "n", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("n")
  }

  test("should not plan a count for node count when there is a predicate on the node") {
    // When
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE n.prop", "n", solveds, cardinalities)
    // Then
    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should not plan a count for node count when there is a predicate on something else") {
    // When
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE 1 = 2", "n", solveds, cardinalities)
    // Then
    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on the relationship") {
    // When
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE r.prop", "r", solveds, cardinalities)
    // Then
    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on something else") {
    // When
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE 1 = 2", "r", solveds, cardinalities)
    // Then
    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should plan a count for node count with a label") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label)", "n", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("n")
  }

  test("should not plan a count for node count with more than one label") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (n:Label1:Label2)", "n", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should plan a count for rel count with no direction") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should plan a count for rel count with no type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r]->()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and no type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r]->()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and no type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()<-[r]-(:Label1)", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count with both ended labels") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r]-(:Label2)", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should not plan a count for rel count with type but no direction") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]-()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  test("should plan a count for rel count with rel type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]->()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and rel type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r:X]->()", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and rel type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH ()<-[r:X]-(:Label1)", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count with both ended labels and rel type") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(mock[PlanContext])
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r:X]-(:Label2)", "r", solveds, cardinalities)

    countStorePlanner(plannerQuery, context, solveds, cardinalities) should notBeCountPlan
  }

  private def producePlannerQuery(query: String, variable: String, solveds: Solveds, cardinalities: Cardinalities) : PlannerQuery = {
    val (pq, _) = producePlannerQueryForPattern(query, solveds, cardinalities)
    pq.withHorizon(AggregatingQueryProjection(
      aggregationExpressions = Map(s"count($variable)" -> FunctionInvocation(FunctionName("count") _, Variable(variable) _) _)))
  }

  case class IsCountPlan(variable: String, noneExpected: Boolean) extends Matcher[Option[LogicalPlan]] {

    override def apply(plan: Option[LogicalPlan]): MatchResult = {
      if (noneExpected) {
        MatchResult(plan.isEmpty, s"Got a plan when none expected: ${plan.getOrElse("")}", "")
      } else {
        MatchResult(
          plan match {
            case Some(NodeCountFromCountStore(countId, _, _)) if countId == s"count($variable)" =>
              true
            case Some(RelationshipCountFromCountStore(countId, _, _, _, _)) if countId == s"count($variable)" =>
              true
            case _ =>
              false
          }, "No count store plan produced", "")
      }
    }
  }

  private def beCountPlanFor(variable: String) = IsCountPlan(variable, false)
  private def notBeCountPlan = new IsCountPlan("", true)

}
