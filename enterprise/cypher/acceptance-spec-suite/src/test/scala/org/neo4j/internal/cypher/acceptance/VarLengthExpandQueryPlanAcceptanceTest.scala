/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperatorWithText("VarLengthExpand(All)", "(e)<-[:*..4]-(a)")
    result.executionPlanDescription() should useOperatorWithText("NodeByLabelScan", ":To")
  }

  test("Plan should have right relationship direction other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperatorWithText("VarLengthExpand(All)", "(a)-[:*..4]->(e)")
    result.executionPlanDescription() should useOperatorWithText("NodeByLabelScan", ":From")
  }

  test("Plan pruning var expand on distinct var-length match") {
    val query = "MATCH (a)-[*1..2]->(c) RETURN DISTINCT c"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("Plan pruning var expand on distinct var-length match with projection and aggregation") {
    val query = "MATCH (a)-[*1..2]->(c) WITH DISTINCT c RETURN count(*)"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("query with distinct aggregation") {
    val query = "MATCH (from)-[*1..3]->(to) RETURN count(DISTINCT to)"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("Simple query that filters between expand and distinct") {
    val query = "MATCH (a)-[*1..3]->(b:X) RETURN DISTINCT b"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("Query that aggregates before making the result DISTINCT") {
    val query = "MATCH (a)-[:R*1..3]->(b) WITH count(*) AS count RETURN DISTINCT count"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(All)")
  }

  test("Double var expand with distinct result") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T*1..3]->(c) RETURN DISTINCT c"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("var expand followed by normal expand") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("optional match can be solved with PruningVarExpand") {
    val query = "MATCH (a) OPTIONAL MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(Pruning)")
  }

  test("should not rewrite when doing non-distinct aggregation") {
    val query = "MATCH (a)-[*1..3]->(b) RETURN b, count(*)"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(All)")
  }

  test("on longer var-lengths, we use FullPruningVarExpand") {
    val query = "MATCH (a)-[*4..5]->(b) RETURN DISTINCT b"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(FullPruning)")
  }

  test("Do not plan pruning var expand for length=1") {
    val query = "MATCH (a)-[*1..1]->(b) RETURN DISTINCT b"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(All)")
  }

  test("Do not plan pruning var expand when path is needed") {
    val query = "MATCH p=(from)-[r*0..1]->(to) WITH nodes(p) AS d RETURN DISTINCT d"
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    result.executionPlanDescription() should useOperators("VarLengthExpand(All)")
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
