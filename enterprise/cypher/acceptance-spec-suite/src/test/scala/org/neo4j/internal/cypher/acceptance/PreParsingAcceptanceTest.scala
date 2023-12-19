/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
