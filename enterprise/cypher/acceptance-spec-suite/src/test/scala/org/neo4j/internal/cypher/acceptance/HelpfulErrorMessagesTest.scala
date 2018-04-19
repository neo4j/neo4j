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

import org.neo4j.cypher.{ExecutionEngineFunSuite, SyntaxException}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should provide sensible error message when omitting colon before relationship type on create") {
    failWithError(Configs.AbsolutelyAll - Configs.Version2_3,
      "CREATE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on create") {
    failWithError(Configs.AbsolutelyAll,
      "CREATE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for CREATE",
          "The given query is not currently supported in the selected cost-based planner" ))
  }

  test("should provide sensible error message when omitting colon before relationship type on merge") {
    failWithError(Configs.AbsolutelyAll - Configs.Version2_3,
      "MERGE (a)-[ASSOCIATED_WITH]->(b)",
      Seq("Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?"))
  }

  test("should provide sensible error message when trying to add multiple relationship types on merge") {
    failWithError(Configs.AbsolutelyAll - Configs.Rule2_3,
      "MERGE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)",
      Seq("A single relationship type must be specified for MERGE",
      "The given query is not currently supported in the selected cost-based planner"))
  }

  test("should provide sensible error message for 3.3 rule planner") {
    intercept[Exception](graph.execute("CYPHER 3.3 planner=rule RETURN 1")).getMessage should be("Unsupported PLANNER - VERSION combination: rule - 3.3")
  }

  test("should provide sensible error message for 3.2 rule planner") {
    intercept[Exception](graph.execute("CYPHER 3.2 planner=rule RETURN 1")).getMessage should be("Unsupported PLANNER - VERSION combination: rule - 3.2")
  }

  test("should not fail for specifying rule planner if no version specified") {
    graph.execute("CYPHER planner=rule RETURN 1") // should not fail
  }

  test("should provide sensible error message for rule planner and slotted") {
    intercept[Exception](graph.execute("CYPHER planner=rule runtime=slotted RETURN 1")).getMessage should be("Unsupported PLANNER - RUNTIME combination: rule - slotted")
  }

  test("should provide sensible error message for invalid regex syntax together with index") {

    graph.execute("CREATE (n:Person {text:'abcxxxdefyyyfff'})")
    failWithError(Configs.Version3_3 + Configs.Version3_2 + Configs.Procs - Configs.Compiled - Configs.AllRulePlanners,
      "MATCH (x:Person) WHERE x.text =~ '*xxx*yyy*' RETURN x.text", List("Invalid Regex:"))
  }

  test("should give correct error message with invalid number literal in a subtract") {
    a[SyntaxException] shouldBe thrownBy {
      innerExecuteDeprecated("with [1a-1] as list return list", Map())
    }
  }
}
