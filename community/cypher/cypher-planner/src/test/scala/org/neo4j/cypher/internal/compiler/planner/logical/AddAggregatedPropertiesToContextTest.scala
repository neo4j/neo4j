/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class AddAggregatedPropertiesToContextTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val context: LogicalPlanningContext = newMockedLogicalPlanningContext(newMockedPlanContext())
  val planSingeQuery: PlanSingleQuery = new PlanSingleQuery

  test("should return input context if no aggregation in horizion") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN n.prop")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return input context if aggregation with grouping in horizion") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop), n.prop")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return input context if no properties could be extracted") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(size(n.prop))")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return input context if query has mutating patterns before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n:Label) CREATE (:NewLabel) RETURN min(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return input context if query has mutating patterns before renaming") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n:Label) CREATE (:NewLabel) WITH n.prop AS prop RETURN min(prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return input context for merge before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MERGE (n:Label) RETURN min(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextNotUpdated(result)
  }

  test("should return updated context if no mutating patterns before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) RETURN min(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context if no mutating patterns before projection followed by aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WITH n.prop AS prop RETURN min(prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context if mutating patterns after aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n:Label) WITH min(n.prop) AS min CREATE (:NewLabel) RETURN min")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context for unwind before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("UNWIND [1,2,3] AS i MATCH (n) RETURN min(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context for distinct before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) WITH DISTINCT n.prop AS prop RETURN min(prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context for LOAD CSV before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("LOAD CSV WITH HEADERS FROM '$url' AS row MATCH (n) WHERE toInteger(row.Value) > 20 RETURN count(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  test("should return updated context for procedure call before aggregation") {
    val plannerQuery = buildSinglePlannerQuery("MATCH (n) CALL db.labels() YIELD label RETURN count(n.prop)")
    val result = planSingeQuery.addAggregatedPropertiesToContext(plannerQuery, context)

    assertContextUpdated(result, Set(("n", "prop")))
  }

  private def assertContextNotUpdated(newContext: LogicalPlanningContext): Unit = {
    newContext.aggregatingProperties should be(empty)
    newContext should equal(context)
  }

  private def assertContextUpdated(newContext: LogicalPlanningContext, expected: Set[(String, String)]): Unit = {
    newContext.aggregatingProperties should equal(expected)
    newContext should not equal context
  }
}
