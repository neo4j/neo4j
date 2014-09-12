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
      assertVersion(v, "create() return 1", v.name )
    }
  }

  test("should use cost by default in 2.2") {
    assertVersion(CypherVersion.v2_2, "match n return n", CypherVersion.v2_2_cost.name)
  }

  test("should use 2.2-cost when possible in 2.2") {
    assertVersion(CypherVersion.v2_2_cost, "match n return n", CypherVersion.v2_2_cost.name)
  }

  test("should fallback to 2.2-rule in 2.2") {
    assertVersion(CypherVersion.v2_2_cost, "create() return 1", CypherVersion.v2_2_rule.name)
  }

  test("should use 2.2-rule if we really ask for it in 2.2") {
    assertVersion(CypherVersion.v2_2_rule, "match n return n", CypherVersion.v2_2_rule.name)
  }


  def assertVersion(v: CypherVersion, query: String, expectedCompiler: String) {
    val executionResult = eengine.profile(s"cypher ${v.name} ${query}").executionPlanDescription()
    val planDescription = executionResult.asJava

    planDescription.getArguments.get("version") should equal(s"CYPHER ${expectedCompiler}")
  }

}
