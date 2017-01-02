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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.ExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_2.CompatibilityPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2._
import org.scalatest.matchers.{MatchResult, Matcher}

class PreParsingAcceptanceTest extends ExecutionEngineFunSuite {

  test("specifying no planner should provide IDP") {
    val query = "PROFILE RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(IDPPlannerName)
  }

  test("specifying cost planner should provide IDP") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(DPPlannerName)
  }

  test("specifying cost planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP using old syntax") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    eengine.execute(query, Map.empty[String,Any]) should havePlanner(DPPlannerName)
  }

  private def havePlanner(expected: PlannerName): Matcher[ExecutionResult] = new Matcher[ExecutionResult] {
    override def apply(result: ExecutionResult): MatchResult = {
      // exhaust the iterator so we can collect the plan description
      result.length
      result.executionPlanDescription() match {
        case CompatibilityPlanDescription(_, _, actual, _) =>
          MatchResult(
            matches = actual == expected,
            rawFailureMessage = s"PlannerName should be $expected, but was $actual",
            rawNegatedFailureMessage = s"PlannerName should not be $actual"
          )
        case planDesc =>
          val s: String = s"Plan description should be of type ${classOf[CompatibilityPlanDescription].getSimpleName}, but was ${planDesc.getClass.getSimpleName}"
          MatchResult(
            matches = false,
            rawFailureMessage = s,
            rawNegatedFailureMessage = s
          )
      }
    }
  }
}
