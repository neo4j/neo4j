/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LogicalPlanningContext, Metrics, QueryGraphProducer, QueryGraphSolver}
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.{AggregatingQueryProjection, IdName}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.v3_4.expressions.{FunctionInvocation, FunctionName, Variable}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, NodeCountFromCountStore, RelationshipCountFromCountStore}
import org.scalatest.matchers.{MatchResult, Matcher}

class countStorePlannerTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer with AstConstructionTestSupport {

  test("should ignore tail") {
    val pq = producePlannerQuery("MATCH (n)", "n")

    countStorePlanner(pq.withTail(null)) should beCountPlanFor("n")
  }

  test("should plan a count for node count no labels") {
    val plannerQuery = producePlannerQuery("MATCH (n)", "n")

    countStorePlanner(plannerQuery) should beCountPlanFor("n")
  }

  test("should plan a count for cartesian node count no labels") {
    val plannerQuery = producePlannerQuery("MATCH (n), (m)", "n")

    countStorePlanner(plannerQuery) should beCountPlanFor("n")
  }

  test("should not plan a count for node count when there is a predicate on the node") {
    // When
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE n.prop", "n")
    // Then
    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should not plan a count for node count when there is a predicate on something else") {
    // When
    val plannerQuery = producePlannerQuery("MATCH (n) WHERE 1 = 2", "n")
    // Then
    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on the relationship") {
    // When
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE r.prop", "r")
    // Then
    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should not plan a count for relationship count when there is a predicate on something else") {
    // When
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-() WHERE 1 = 2", "r")
    // Then
    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should plan a count for node count with a label") {
    val plannerQuery = producePlannerQuery("MATCH (n:Label)", "n")

    countStorePlanner(plannerQuery) should beCountPlanFor("n")
  }

  test("should not plan a count for node count with more than one label") {
    val plannerQuery = producePlannerQuery("MATCH (n:Label1:Label2)", "n")

    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should plan a count for rel count with no direction") {
    val plannerQuery = producePlannerQuery("MATCH ()-[r]-()", "r")

    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should plan a count for rel count with no type") {
    val plannerQuery = producePlannerQuery("MATCH ()-[r]->()", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and no type") {
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r]->()", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and no type") {
    val plannerQuery = producePlannerQuery("MATCH ()<-[r]-(:Label1)", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count with both ended labels") {
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r]-(:Label2)", "r")

    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should not plan a count for rel count with type but no direction") {
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]-()", "r")

    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  test("should plan a count for rel count with rel type") {
    val plannerQuery = producePlannerQuery("MATCH ()-[r:X]->()", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should plan a count for rel count with lhs label and rel type") {
    val plannerQuery = producePlannerQuery("MATCH (:Label1)-[r:X]->()", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should plan a count for node count with rhs label and rel type") {
    val plannerQuery = producePlannerQuery("MATCH ()<-[r:X]-(:Label1)", "r")

    countStorePlanner(plannerQuery) should beCountPlanFor("r")
  }

  test("should not plan a count for rel count with both ended labels and rel type") {
    val plannerQuery = producePlannerQuery("MATCH (:Label1)<-[r:X]-(:Label2)", "r")

    countStorePlanner(plannerQuery) should notBeCountPlan
  }

  implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[Metrics.CardinalityModel]),
    mock[Metrics], SemanticTable(), mock[QueryGraphSolver], notificationLogger = mock[InternalNotificationLogger])

  def producePlannerQuery(query: String, variable: String) = {
    val (pq, _) = producePlannerQueryForPattern(query)
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
            case Some(NodeCountFromCountStore(IdName(countId), _, _)) if countId == s"count($variable)" =>
              true
            case Some(RelationshipCountFromCountStore(IdName(countId), _, _, _, _)) if countId == s"count($variable)" =>
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
