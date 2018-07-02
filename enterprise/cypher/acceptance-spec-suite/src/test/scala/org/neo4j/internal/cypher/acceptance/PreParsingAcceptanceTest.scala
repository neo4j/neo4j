/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.planner.v3_5.spi.{DPPlannerName, IDPPlannerName}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.opencypher.v9_0.frontend.PlannerName
import org.scalatest.matchers.Matcher

class PreParsingAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not use eagerness when option not provided ") {
    execute("MATCH () CREATE ()").executionPlanDescription() shouldNot includeSomewhere.aPlan("Eager")
  }

  test("should use eagerness when option is provided ") {
    execute("CYPHER updateStrategy=eager MATCH () CREATE ()").executionPlanDescription() should includeSomewhere.aPlan("Eager")
  }

  test("specifying no planner should provide IDP") {
    val query = "PROFILE RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying cost planner should provide IDP") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(DPPlannerName)
  }

  test("specifying cost planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=cost RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying idp planner should provide IDP using old syntax") {
    val query = "PROFILE CYPHER planner=idp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(IDPPlannerName)
  }

  test("specifying dp planner should provide DP using old syntax") {
    val query = "PROFILE CYPHER planner=dp RETURN 1"

    execute(query).executionPlanDescription() should havePlanner(DPPlannerName)
  }

  private def havePlanner(expected: PlannerName): Matcher[InternalPlanDescription] =
    haveAsRoot.aPlan.containingArgument(expected.name)
}
