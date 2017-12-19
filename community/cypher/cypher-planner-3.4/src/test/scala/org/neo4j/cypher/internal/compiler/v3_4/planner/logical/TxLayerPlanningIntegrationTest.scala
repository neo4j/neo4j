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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.scalatest.matchers.{MatchResult, Matcher}

class TxLayerPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("MATCH and CREATE") {
    val plan = planFor("MATCH (a) CREATE (b)")._2
    plan.asInstanceOf[EmptyResult] should readFromLayer(1)
    plan.lhs.get.asInstanceOf[CreateNode] should writeToLayer(1)
    plan.lhs.get.lhs.get.asInstanceOf[AllNodesScan] should readFromLayer(0)
  }

  test("MATCH SET RETURN") {
    val plan = planFor("MATCH (n)-[r]-(m) SET r.val = r.val + 1 RETURN r.val AS rv")._2
    plan.asInstanceOf[Projection] should readFromLayer(1)
    plan.lhs.get.lhs.get.asInstanceOf[SetRelationshipPropery] should writeToLayer(1)
    plan.lhs.get.lhs.get.lhs.get.asInstanceOf[Expand] should readFromLayer(0)
  }

  test("MATCH and triple SET") {
    val plan = planFor("MATCH (a:A),(b:B),(c:C) SET a.prop = b.prop SET b.prop = c.prop SET c.prop = 42")._2
    plan.asInstanceOf[EmptyResult] should readFromLayer(3)
    plan.lhs.get.asInstanceOf[SetNodeProperty] should writeToLayer(3)
    plan.lhs.get.lhs.get.asInstanceOf[SetNodeProperty] should writeToLayer(2)
    plan.lhs.get.lhs.get.lhs.get.asInstanceOf[SetNodeProperty] should writeToLayer(1)
    plan.lhs.get.lhs.get.lhs.get.lhs.get.asInstanceOf[CartesianProduct] should readFromLayer(0)
  }

  test("DELETE and MERGE") {
    val plan = planFor("MATCH (n) DELETE n MERGE (m {p: 0}) ON CREATE SET m.p = 1 RETURN count(*)")._2
    plan.asInstanceOf[Aggregation] should readFromLayer(2)
    plan.lhs.get.rhs.get.lhs.get.lhs.get.lhs.get.asInstanceOf[AllNodesScan] should readFromLayer(1) // MATCH part of merge
    plan.lhs.get.rhs.get.rhs.get.asInstanceOf[SetNodeProperty] should writeToLayer(2) // Set property for CREATE part of merge
    plan.lhs.get.lhs.get.lhs.get.asInstanceOf[DeleteNode] should writeToLayer(1)
    plan.lhs.get.lhs.get.lhs.get.lhs.get.asInstanceOf[AllNodesScan] should readFromLayer(0) // First match
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

}




