/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

  test("should use cost by default in 2.2") {
    given("match n return n")
      .shouldHaveCypherVersion(CypherVersion.v2_2_cost)
  }

  test("should fallback to 2.2-rule in 2.2") {
    given("create() return 1")
      .withCypherVersion(CypherVersion.v2_2_cost)
      .shouldHaveCypherVersion(CypherVersion.v2_2_rule)
  }


  test("should use 2.2-rule if we really ask for it in 2.2") {
    given("match n return n")
      .withCypherVersion(CypherVersion.v2_2_rule)
      .shouldHaveCypherVersion(CypherVersion.v2_2_rule)
  }

  test("children should be empty") {
    given("match n return n").planDescripton.getChildren.size() should equal(0)
  }

  test("DbHits should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("DbHits") should equal("1")
  }

  test("Rows should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("Rows") should equal("0")
  }

  test("EstimatedRows should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("EstimatedRows") should equal("0")
  }

  test("IntroducedIdentifier should be properly formatted") {
    given("match n return n").planDescripton.getArguments.get("IntroducedIdentifier") should equal("n")
  }

  def given(query: String) = TestQuery(query)

  case class TestQuery(query: String,
                       cypherVersion: Option[CypherVersion] = None) {

    def withCypherVersion(version: CypherVersion) = copy(cypherVersion = Some(version))

    def shouldHaveCypherVersion(version: CypherVersion) = {
      val planDescription = execute()
      planDescription.getArguments.get("version") should equal(s"CYPHER ${version.name}")

      this
    }

    def planDescripton = execute()

    private def execute() = {
      val version = cypherVersion match {
        case Some(v) => s"cypher ${v.name}"
        case None => ""
      }
      val executionResult = eengine.profile(s"${version}  ${query}").executionPlanDescription()
      executionResult.asJava
    }
  }

}
