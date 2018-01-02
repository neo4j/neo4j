/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.frontend.v3_4.PlannerName
import org.neo4j.cypher.internal.planner.v3_4.spi.{DPPlannerName, IDPPlannerName}
import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.scalatest.matchers.{MatchResult, Matcher}

class PreParsingAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not use eagerness when option not provided ") {
    execute("MATCH () CREATE ()") shouldNot use("Eager")
  }

  test("should use eagerness when option is provided ") {
    execute("CYPHER updateStrategy=eager MATCH () CREATE ()") should use("Eager")
  }

  test("specifying no planner should provide IDP") {
    val query = "PROFILE RETURN 1"

    execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying cost planner should provide IDP") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query) should havePlanner(DPPlannerName)
  }

  test("specifying cost planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query) should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP using old syntax") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query) should havePlanner(DPPlannerName)
  }

  private def havePlanner(expected: PlannerName): Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      // exhaust the iterator so we can collect the plan description
      result.length
      result.executionPlanDescription() match {
        case planDesc =>
          val actual = planDesc.arguments.collectFirst {
            case Arguments.PlannerImpl(name) => name
          }
          MatchResult(
            matches = actual.isDefined && actual.get == expected.name,
            rawFailureMessage = s"PlannerName should be $expected, but was $actual",
            rawNegatedFailureMessage = s"PlannerName should not be $actual"
          )
      }
    }
  }
}
