/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_2.{Cost, PlannerName, Rule}

class RootPlanAcceptanceTest extends ExecutionEngineFunSuite {

  test("should include version information in root plan description for queries of each legacy version") {

    //v2.2 must be handled separately since the resulting compiler is dependent on query
    val versions = CypherVersion.allVersions.filter(!_.name.startsWith(CypherVersion.v2_2.name))
    versions.foreach { v =>
      given("create() return 1")
        .withCypherVersion(v)
        .shouldHaveCypherVersion(v)
    }
  }

  test("should use Cost if it can by default in 2.2") {
    given("match n return n")
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Cost)
  }
  test("should use Rule for varlength in 2.2") {
    given("match (a)-[r:T1*]->(b) return a,r,b")
      .withCypherVersion(CypherVersion.v2_2)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Rule)
  }

  test("should use Rule for loops in 2.2") {
    given("match (a)-[r]->(a) return a")
      .withCypherVersion(CypherVersion.v2_2)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Rule)
  }

  test("should fallback to Rule for updates in 2.2") {
    given("create() return 1")
      .withCypherVersion(CypherVersion.v2_2)
      .withPlannerName(Cost)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Rule)
  }

  test("should use rule if we really ask for it in 2.2") {
    given("match n return n")
      .withCypherVersion(CypherVersion.v2_2)
      .withPlannerName(Rule)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Rule)
  }

  test("should use cost if we really ask for it in 2.2") {
    given("match n return n")
      .withCypherVersion(CypherVersion.v2_2)
      .withPlannerName(Cost)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Cost)
  }

  test("should fail when using planner together with older versions") {
    intercept[InvalidArgumentException] {
      given("match n return n")
        .withCypherVersion(CypherVersion.v2_1)
        .withPlannerName(Cost)
        .planDescripton
    }
  }

  test("should report RULE if we ask it for UNION queries") {
    given(
      """MATCH p=(n:Person {first_name: 'Shawna'})-[:FRIEND_OF]-(m:Person)
        |RETURN p UNION MATCH p=(n:Person {first_name: 'Shawna'})-[:FRIEND_OF]-()-[:FRIEND_OF]-(m:Person) RETURN p""".stripMargin)
      .withCypherVersion(CypherVersion.v2_2)
      .withPlannerName(Rule)
      .shouldHaveCypherVersion(CypherVersion.v2_2)
      .shouldHavePlannerName(Rule)
  }

  test("children should be empty") {
    given("match n return n").planDescripton.getChildren.size() should equal(0)
  }

  test("DbHits should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("DbHits") should equal(1)
  }

  test("Rows should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("Rows") should equal(0)
  }

  test("EstimatedRows should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("EstimatedRows") should equal(0)
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None,
                        planner: Option[PlannerName] = None) {

    def withCypherVersion(version: CypherVersion) = copy(cypherVersion = Some(version))
    def withPlannerName(planner: PlannerName) = copy(planner = Some(planner))

    def shouldHaveCypherVersion(version: CypherVersion) = {
      val planDescription = execute()
      planDescription.getArguments.get("version") should equal(s"CYPHER ${version.name}")

      this
    }

    def shouldHavePlannerName(planner: PlannerName) = {
      val planDescription = execute()
      planDescription.getArguments.get("planner") should equal(s"${planner.name}")

      this
    }

    def planDescripton = execute()

    private def execute() = {
      val versionString = cypherVersion match {
        case Some(v) => s"cypher ${v.name}"
        case None => ""
      }
      val plannerString = planner match {
              case Some(p) => s"planner ${p.name}"
              case None => ""
            }
      val executionResult = eengine.profile(s"${versionString}  ${plannerString} ${query}").executionPlanDescription()
      executionResult.asJava
    }
  }

}
