/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.graphdb.{Label, Node, RelationshipType}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}

class QueryPlanCompatibilityTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should produce compatible plans for simple MATCH node query") {
    val query = "MATCH (n:Person) RETURN n"
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan), expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should produce compatible plans for simple MATCH relationship query") {
    val query = "MATCH (n:Person)-[r:KNOWS]->(m) RETURN r"
    executeWith(Configs.All, query)
  }

  test("should produce compatible plans with predicates") {
    val query =
      """
        |MATCH (n:Person) WHERE n.name STARTS WITH 'Joe' AND n.age >= 42
        |RETURN count(n)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan), expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should produce compatible plans with unwind") {
    val query =
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |RETURN count(n)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan), expectPlansToFail = Configs.AllRulePlanners))
  }

  // Too much has changed from 2.3, only compare plans for newer versions
  test("should produce compatible plans for complex query") {
    val query =
      """
        |WITH 'Joe' as name
        |UNWIND [42,43,44] as age
        |MATCH (n:Person) WHERE n.name STARTS WITH name AND n.age >= age
        |OPTIONAL MATCH (n)-[r:KNOWS]->(m) WHERE exists(r.since)
        |RETURN count(r)
      """.stripMargin
    val expectedPlan = generateExpectedPlan(query)
    executeWith(Configs.Interpreted, query,
      planComparisonStrategy = ComparePlansWithAssertion(assertSimilarPlans(_, expectedPlan), expectPlansToFail = Configs.AllRulePlanners + Configs.Version2_3))
  }

  private def assertSimilarPlans(plan: InternalPlanDescription, expected: InternalPlanDescription): Unit = {
    plan.flatten.map(simpleName).toString should equal(expected.flatten.map(simpleName).toString())
  }

  private def generateExpectedPlan(query: String): InternalPlanDescription = innerExecuteDeprecated(query, Map.empty).executionPlanDescription()

  private def simpleName(plan: InternalPlanDescription): String = plan.name.replace("SetNodeProperty", "SetProperty").toLowerCase
}
