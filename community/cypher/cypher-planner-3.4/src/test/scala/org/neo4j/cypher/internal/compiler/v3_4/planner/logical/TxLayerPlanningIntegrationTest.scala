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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.scalatest.matchers.{MatchResult, Matcher}

class TxLayerPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH and CREATE") {
    val plan = planFor("MATCH (a) CREATE (b)")._2
    inSeqAssert(plan,
      (classOf[AllNodesScan], _ should readFromLayer(0)),
      (classOf[CreateNode], _ should writeToLayer(1)),
      (classOf[EmptyResult], _ should readFromLayer(1))
    )
  }

  test("MATCH SET RETURN") {
    val plan = planFor("MATCH (n)-[r]-(m) SET r.val = r.val + 1 RETURN r.val AS rv")._2
    inSeqAssert(plan,
      (classOf[Expand], _ should readFromLayer(0)),
      (classOf[SetRelationshipPropery], _ should writeToLayer(1)),
      (classOf[Projection], _ should readFromLayer(1))
    )
  }

  test("MATCH and triple SET") {
    val plan = planFor("MATCH (a:A),(b:B),(c:C) SET a.prop = b.prop SET b.prop = c.prop SET c.prop = 42")._2
    inSeqAssert(plan,
      (classOf[CartesianProduct], _ should readFromLayer(0)),
      (classOf[SetNodeProperty], _ should writeToLayer(1)),
      (classOf[SetNodeProperty], _ should writeToLayer(2)),
      (classOf[SetNodeProperty], _ should writeToLayer(3)),
      (classOf[EmptyResult], _ should readFromLayer(3))
    )
  }

  test("DELETE and MERGE") {
    val plan = planFor("MATCH (n) DELETE n MERGE (m {p: 0}) ON CREATE SET m.p = 1 RETURN count(*)")._2
    inSeqAssert(plan,
      (classOf[AllNodesScan], _ should readFromLayer(0)), // First match
      (classOf[DeleteNode], _ should writeToLayer(1)),
      (classOf[AllNodesScan], _ should readFromLayer(1)), // MATCH part of merge
      (classOf[SetNodeProperty], _ should writeToLayer(2)), // Set property for CREATE part of merge
      (classOf[Aggregation], _ should readFromLayer(2))
    )
  }

  def readFromLayer(i: Int): Matcher[LogicalPlan] = new Matcher[LogicalPlan] {
    override def apply(plan: LogicalPlan): MatchResult = {
      MatchResult(
        matches = plan.readTransactionLayer.value == i,
        rawFailureMessage = s"Plan $plan should read from $i but reads from ${plan.readTransactionLayer.value}",
        rawNegatedFailureMessage = s"Plan $plan should read not from $i"
      )
    }
  }

  def writeToLayer(i: Int): Matcher[LogicalPlan] = new Matcher[LogicalPlan] {
    override def apply(plan: LogicalPlan): MatchResult = {
      MatchResult(
        matches = plan.readTransactionLayer.value + 1 == i,
        rawFailureMessage = s"Plan $plan should write to $i but writes to ${plan.readTransactionLayer.value + 1}",
        rawNegatedFailureMessage = s"Plan $plan should read write to $i"
      )
    }
  }

  def depthFirstLeftToRight(lp: LogicalPlan): List[LogicalPlan] = {
    lp.lhs.fold(List.empty[LogicalPlan])(depthFirstLeftToRight) ++
      lp.rhs.fold(List.empty[LogicalPlan])(depthFirstLeftToRight) :+
      lp
  }

  def inSeqAssert(plan: LogicalPlan, assertions: (Class[_ <: LogicalPlan], (LogicalPlan) => Unit)*): Unit = {
    val plans = depthFirstLeftToRight(plan)

    def recAssert(restOfPlans: List[LogicalPlan], restOfAssertions: List[(Class[_ <: LogicalPlan], (LogicalPlan) => Unit)]): Unit = (restOfPlans, restOfAssertions) match {
      case (_, Nil) => //Done
      case (Nil, as :: _) => fail(s"Expected ${as._1.getSimpleName} in the rest of the plan, but not found")
      case (p :: moreP, as :: moreA) if p.getClass == as._1 =>
        val assertIt = as._2
        assertIt(p)
        recAssert(moreP, moreA)
      case (_ :: moreP, _) => recAssert(moreP, restOfAssertions)
    }

    recAssert(plans, assertions.toList)
  }

}




