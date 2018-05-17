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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private val expectedToSucceed = Configs.Interpreted

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"

    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperatorWithText("VarLengthExpand(All)", "(e)<-[:*..4]-(a)")
        plan should useOperatorWithText("NodeByLabelScan", ":To")
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))
  }

  test("Plan should have right relationship direction, other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperatorWithText("VarLengthExpand(All)", "(a)-[:*..4]->(e)")
        plan should useOperatorWithText("NodeByLabelScan", ":From")
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))
  }

  test("Plan pruning var expand on distinct var-length match") {
    val query = "MATCH (a)-[*1..2]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("Plan pruning var expand on distinct var-length match with projection and aggregation") {
    val query = "MATCH (a)-[*1..2]->(c) WITH DISTINCT c RETURN count(*)"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("query with distinct aggregation") {
    val query = "MATCH (from)-[*1..3]->(to) RETURN count(DISTINCT to)"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("Simple query that filters between expand and distinct") {
    val query = "MATCH (a)-[*1..3]->(b:X) RETURN DISTINCT b"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("Query that aggregates before making the result DISTINCT") {
    val query = "MATCH (a)-[:R*1..3]->(b) WITH count(*) AS count RETURN DISTINCT count"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("Double var expand with distinct result") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T*1..3]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("var expand followed by normal expand") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("optional match can be solved with PruningVarExpand") {
    val query = "MATCH (a) OPTIONAL MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("should not rewrite when doing non-distinct aggregation") {
    val query = "MATCH (a)-[*1..3]->(b) RETURN b, count(*)"
    executeWith(Configs.Interpreted + Configs.Cost2_3, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("on longer var-lengths, we also use PruningVarExpand") {
    val query = "MATCH (a)-[*4..5]->(b) RETURN DISTINCT b"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = Configs.OldAndRule))
  }

  test("Do not plan pruning var expand for length=1") {
    val query = "MATCH (a)-[*1..1]->(b) RETURN DISTINCT b"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  test("Do not plan pruning var executeWithCostPlannerAndInterpretedRuntimeOnly when path is needed") {
    val query = "MATCH p=(from)-[r*0..1]->(to) WITH nodes(p) AS d RETURN DISTINCT d"
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
  }

  private def setUp(startLabel: String) {
    val a = createLabeledNode(Map("name" -> "Keanu Reeves"), "From")
    val b = createLabeledNode(Map("name" -> "Craig"), "User")
    val c = createLabeledNode(Map("name" -> "Olivia"), "User")
    val d = createLabeledNode(Map("name" -> "Carrie"), "User")
    val e = createLabeledNode(Map("name" -> "Andres"), "To")
    // Ensure compiler prefers to start at low cardinality 'To' node
    Range(0, 100).foreach(i => createLabeledNode(Map("name" -> s"node $i"), startLabel))
    relate(a, b)
    relate(b, c)
    relate(c, d)
    relate(d, e)
  }
}
