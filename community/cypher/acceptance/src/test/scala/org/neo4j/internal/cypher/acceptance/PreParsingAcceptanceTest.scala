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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compatibility.{CompatibilityPlanDescription, CompatibilityPlanDescriptionFor2_3}
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_2
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExtendedExecutionResult}
import org.scalatest.matchers.{MatchResult, Matcher}

class PreParsingAcceptanceTest extends ExecutionEngineFunSuite {

  test("specifying no planner should provide IDP") {
    val query = "PROFILE RETURN 1"

    eengine.execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying cost planner should provide IDP") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    eengine.execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    eengine.execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying greedy planner should provide greedy") {
    val query = "PROFILE CYPHER planner=greedy RETURN 1"

    eengine.execute(query) should havePlanner(GreedyPlannerName)
  }

  test("specifying dp planner should provide DP") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    eengine.execute(query) should havePlanner(DPPlannerName)
  }

  test("specifying rule planner should provide RULE") {
    val query = "PROFILE CYPHER planner=rule RETURN 1"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying no planner in a write query should provide RULE") {
    val query = "PROFILE CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying cost planner in a write query should fallback to RULE") {
    val query = "PROFILE CYPHER planner=cost CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying cost planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner cost RETURN 1"

    eengine.execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner idp RETURN 1"

    eengine.execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying greedy planner should provide greedy using old syntax") {
    val query = "PROFILE CYPHER planner greedy RETURN 1"

    eengine.execute(query) should havePlanner(GreedyPlannerName)
  }

  test("specifying dp planner should provide DP using old syntax") {
    val query = "PROFILE CYPHER planner dp RETURN 1"

    eengine.execute(query) should havePlanner(DPPlannerName)
  }

  test("specifying rule planner should provide RULE using old syntax") {
    val query = "PROFILE CYPHER planner rule RETURN 1"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying cost planner in a write query should fallback to RULE using old syntax") {
    val query = "PROFILE CYPHER planner cost CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying idp planner in a write query should fallback to RULE using old syntax") {
    val query = "PROFILE CYPHER planner idp CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying greedy planner in a write query should fallback to RULE using old syntax") {
    val query = "PROFILE CYPHER planner greedy CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying dp planner in a write query should fallback to RULE using old syntax") {
    val query = "PROFILE CYPHER planner dp CREATE ()"

    eengine.execute(query) should havePlanner(RulePlannerName)
  }

  test("specifying no planner in 2.2 compatibility should provide COST") {
    val query = "PROFILE CYPHER 2.2 RETURN 1"

    eengine.execute(query) should havePlanner(v2_2.CostPlannerName)
  }

  test("specifying cost planner in 2.2 compatibility should provide COST") {
    val query = "PROFILE CYPHER 2.2 planner=cost RETURN 1"

    eengine.execute(query) should havePlanner(v2_2.CostPlannerName)
  }

  test("specifying idp planner in 2.2 compatibility should provide IDP") {
    val query = "PROFILE CYPHER 2.2 planner=idp RETURN 1"

    eengine.execute(query) should havePlanner(v2_2.IDPPlannerName)
  }

  test("specifying dp planner in 2.2 compatibility should provide DP") {
    val query = "PROFILE CYPHER 2.2 planner=dp RETURN 1"

    eengine.execute(query) should havePlanner(v2_2.DPPlannerName)
  }

  test("specifying rule planner in 2.2 compatibility should provide RULE") {
    val query = "PROFILE CYPHER 2.2 planner=rule RETURN 1"

    eengine.execute(query) should havePlanner(v2_2.RulePlannerName)
  }

  test("specifying no planner in 2.2 compatibility for a write query should provide RULE") {
    val query = "PROFILE CYPHER 2.2 CREATE ()"

    eengine.execute(query) should havePlanner(v2_2.RulePlannerName)
  }

  private def havePlanner(expected: PlannerName): Matcher[ExtendedExecutionResult] = new Matcher[ExtendedExecutionResult] {
    override def apply(result: ExtendedExecutionResult): MatchResult = {
      // exhaust the iterator so we can collect the plan description
      result.length
      result.executionPlanDescription() match {
        case CompatibilityPlanDescriptionFor2_3(_, _, actual, _) =>
          MatchResult(
            matches = actual == expected,
            rawFailureMessage = s"PlannerName should be $expected, but was $actual",
            rawNegatedFailureMessage = s"PlannerName should not be $actual"
          )
        case planDesc =>
          MatchResult(
            matches = false,
            rawFailureMessage = s"Plan description should be of type CompatibilityPlanDescriptionFor2_3, but was ${planDesc.getClass.getSimpleName}",
            rawNegatedFailureMessage = s"Plan description should be of type CompatibilityPlanDescriptionFor2_3, but was ${planDesc.getClass.getSimpleName}"
          )
      }
    }
  }

  private def havePlanner(expected: v2_2.PlannerName): Matcher[ExtendedExecutionResult] = new Matcher[ExtendedExecutionResult] {
    override def apply(result: ExtendedExecutionResult): MatchResult = {
      // exhaust the iterator so we can collect the plan description
      result.length
      result.executionPlanDescription() match {
        case CompatibilityPlanDescription(_, _, actual) =>
          MatchResult(
            matches = actual == expected,
            rawFailureMessage = s"PlannerName should be $expected, but was $actual",
            rawNegatedFailureMessage = s"PlannerName should not be $actual"
          )
        case planDesc =>
          MatchResult(
            matches = false,
            rawFailureMessage = s"Plan description should be of type CompatibilityPlanDescription, but was ${planDesc.getClass.getSimpleName}",
            rawNegatedFailureMessage = s"Plan description should be of type CompatibilityPlanDescription, but was ${planDesc.getClass.getSimpleName}"
          )
      }
    }
  }

}
