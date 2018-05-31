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

import org.neo4j.cypher.{ExecutionEngineFunSuite, SyntaxException}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

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

  test("should provide sensible error message for 3.5 rule planner") {
    intercept[Exception](graph.execute("CYPHER 3.5 planner=rule RETURN 1")).getMessage should be("Unsupported PLANNER - VERSION combination: rule - 3.5")
  }

  test("should not fail for specifying rule planner if no version specified") {
    graph.execute("CYPHER planner=rule RETURN 1") // should not fail
  }

  test("should provide sensible error message for rule planner and slotted") {
    intercept[Exception](graph.execute("CYPHER planner=rule runtime=slotted RETURN 1")).getMessage should be("Unsupported PLANNER - RUNTIME combination: rule - slotted")
  }

  test("should provide sensible error message for invalid regex syntax together with index") {

    // Fixed in 3.2.8
    graph.execute("CREATE (n:Person {text:'abcxxxdefyyyfff'})")
    failWithError(Configs.AbsolutelyAll - Configs.AllRulePlanners - Configs.Compiled - Configs.Version3_1 - Configs.Version2_3,
      "MATCH (x:Person) WHERE x.text =~ '*xxx*yyy*' RETURN x.text", List("Invalid Regex:"))
  }

  test("should provide sensible error message for START in newer runtimes") {
    val query = "START n=node(0) RETURN n"
    failWithError(Configs.SlottedInterpreted + Configs.Compiled, query, Seq("The given query is not currently supported in the selected runtime"))
  }

  test("should not fail when using compatible runtime with START") {
    createNode()
    val query = "START n=node(0) RETURN n"
    val conf = TestConfiguration(
      Versions(Versions.V2_3, Versions.V3_1, Versions.Default),
      Planners(Planners.Rule, Planners.Default),
      Runtimes(Runtimes.Interpreted, Runtimes.Default))
    executeWith(conf, query) // should not fail
  }

  test("should provide sensible error message for CREATE UNIQUE in newer runtimes") {
    val query = "MATCH (root { name: 'root' }) CREATE UNIQUE (root)-[:LOVES]-(someone) RETURN someone"
    failWithError(Configs.SlottedInterpreted + Configs.Compiled, query, Seq("The given query is not currently supported in the selected runtime"))
  }

  test("should give correct error message with invalid number literal in a subtract") {
    a[SyntaxException] shouldBe thrownBy {
      innerExecuteDeprecated("with [1a-1] as list return list", Map())
    }
  }
}
