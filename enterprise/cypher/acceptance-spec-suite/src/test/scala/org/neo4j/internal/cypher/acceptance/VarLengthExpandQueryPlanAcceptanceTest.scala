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
import org.neo4j.graphdb.Path
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class VarLengthExpandQueryPlanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private val expectedToSucceed = Configs.Interpreted
  private val ignoreConfiguration = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1

  test("Plan should have right relationship direction") {
    setUp("From")
    val query = "MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"

    val ignoreConfiguration = Configs.AllRulePlanners + Configs.Cost2_3
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperatorWithText("VarLengthExpand(All)", "(e)<-[:*..4]-(a)")
        plan should useOperatorWithText("NodeByLabelScan", ":To")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Plan should have right relationship direction other direction") {
    setUp("To")
    val query = "PROFILE MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
    val ignoreConfiguration = Configs.AllRulePlanners + Configs.Cost2_3
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperatorWithText("VarLengthExpand(All)", "(a)-[:*..4]->(e)")
        plan should useOperatorWithText("NodeByLabelScan", ":From")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Plan pruning var expand on distinct var-length match") {
    val query = "MATCH (a)-[*1..2]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Plan pruning var expand on distinct var-length match with projection and aggregation") {
    val query = "MATCH (a)-[*1..2]->(c) WITH DISTINCT c RETURN count(*)"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("query with distinct aggregation") {
    val query = "MATCH (from)-[*1..3]->(to) RETURN count(DISTINCT to)"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Simple query that filters between expand and distinct") {
    val query = "MATCH (a)-[*1..3]->(b:X) RETURN DISTINCT b"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Query that aggregates before making the result DISTINCT") {
    val query = "MATCH (a)-[:R*1..3]->(b) WITH count(*) AS count RETURN DISTINCT count"
    val ignoreConfiguration = Configs.AllRulePlanners
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Double var expand with distinct result") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T*1..3]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("var expand followed by normal expand") {
    val query = "MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("optional match can be solved with PruningVarExpand") {
    val query = "MATCH (a) OPTIONAL MATCH (a)-[:R*1..3]->(b)-[:T]->(c) RETURN DISTINCT c"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(Pruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("should not rewrite when doing non-distinct aggregation") {
    val query = "MATCH (a)-[*1..3]->(b) RETURN b, count(*)"
    val ignoreConfiguration = Configs.AllRulePlanners
    executeWith(Configs.Interpreted + Configs.Cost2_3, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("on longer var-lengths, we use FullPruningVarExpand") {
    val query = "MATCH (a)-[*4..5]->(b) RETURN DISTINCT b"
    executeWith(expectedToSucceed - Configs.SlottedInterpreted, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(FullPruning)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("Do not plan pruning var expand for length=1") {
    val query = "MATCH (a)-[*1..1]->(b) RETURN DISTINCT b"
    val ignoreConfiguration = Configs.AllRulePlanners
    executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion( plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = ignoreConfiguration))
  }

  test("AllNodesInPath") {
    graph.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN nodes(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationships") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3  {foo:'bar'}]->(pB)
                  |WHERE all(i IN rels(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllRelationshipsInPath") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE all(i IN rels(p) WHERE i.foo = 'bar')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoNodeInPath") {
    graph.execute("CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar', name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN nodes(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("NoRelationshipInPath") {
    graph.execute("CREATE (a:A)-[:REL {foo: 'bar'}]->(b:B)-[:REL {foo: 'bar'}]->(c:C)-[:REL {foo: 'bar'}]->(d:D {name: 'd'})")
    val query = """MATCH p = (pA)-[:REL*3..3]->(pB)
                  |WHERE none(i IN rels(p) WHERE i.foo = 'barz')
                  |RETURN pB.name """.stripMargin
    val result = executeWith(expectedToSucceed, query, planComparisonStrategy =
      ComparePlansWithAssertion(plan => {
        plan should useOperators("VarLengthExpand(All)")
      }, expectPlansToFail = Configs.AllRulePlanners))
    result.toList should equal(List(Map("pB.name" -> "d")))
  }

  test("AllNodesInPath with inner predicate using labelled nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using labelled named nodes of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (start:NODE)-[rel*1]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate using nodes of the path") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1,node2)

    val query =
      """
        | MATCH p = ()-[*1]->()
        | WHERE ALL(x IN nodes(p) WHERE single(y IN nodes(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }


  test("AllNodesInPath with complex inner predicate using the start node and end node") {
    val node1 = createLabeledNode(Map("prop" -> 1), "NODE")
    val node2 = createLabeledNode(Map("prop" -> 1),"NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (start:NODE)-[*1..2]->(end:NODE)
        | WHERE ALL(x IN nodes(p) WHERE x.prop = nodes(p)[0].prop AND x.prop = nodes(p)[1].prop)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1 - Configs.Version2_3

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN nodes(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllNodesInPath with inner predicate only using start node") {
    val node1 = createLabeledNode(Map("prop" -> 5),"NODE")
    val node2 = createLabeledNode(Map("prop" -> 5),"NODE")
    relate(node1,node2)

    val query =
      """ MATCH p = (n)-[r*1]->()
        | WHERE ALL(x IN nodes(p) WHERE x.prop = n.prop)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with inner predicate using rels of the path") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN rels(p) WHERE single(y IN rels(p) WHERE y = x))
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("AllRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1]->(:NODE)
        | WHERE ALL(x IN rels(p) WHERE length(p) = 1)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoNodesInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN nodes(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  test("NoRelationshipsInPath with simple inner predicate") {
    val node1 = createLabeledNode("NODE")
    val node2 = createLabeledNode("NODE")
    relate(node1,node2)

    val query =
      """
        | MATCH p = (:NODE)-[*1..2]->(:NODE)
        | WHERE NONE(x IN rels(p) WHERE length(p) = 2)
        | RETURN p
      """.stripMargin

    val configs = Configs.CommunityInterpreted - Configs.Cost3_2 - Configs.Cost3_1

    val result = executeWith(configs, query)
    val path = result.toList.head("p").asInstanceOf[Path]
    path.startNode() should equal(node1)
    path.endNode() should equal(node2)
  }

  //  test("Do not plan pruning var expand when path is needed") {
  //    val query = "MATCH p=(from)-[r*0..1]->(to) WITH nodes(p) AS d RETURN DISTINCT d"
  //    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)
  //
  //    result.executionPlanDescription() should useOperators("VarLengthExpand(All)")
  //=======
  //    val query = "MATCH (a:From {name:'Keanu Reeves'})-[*..4]->(e:To {name:'Andres'}) RETURN *"
  //
  //    val expectedPlan =
  //      """
  //        |+-----------------------+----------------+------------------+--------------------------------+
  //        || Operator              | Estimated Rows | Variables        | Other                          |
  //        |+-----------------------+----------------+------------------+--------------------------------+
  //        || +ProduceResults       |              0 | anon[37], a, e   |                                |
  //        || |                     +----------------+------------------+--------------------------------+
  //        || +Filter               |              0 | anon[37], a, e   | e:To; e.name = {  AUTOSTRING1} |
  //        || |                     +----------------+------------------+--------------------------------+
  //        || +VarLengthExpand(All) |              0 | anon[37], e -- a | (a)-[:*..4]->(e)               |
  //        || |                     +----------------+------------------+--------------------------------+
  //        || +Filter               |              0 | a                | a.name = {  AUTOSTRING0}       |
  //        || |                     +----------------+------------------+--------------------------------+
  //        || +NodeByLabelScan      |              1 | a                | :From                          |
  //        |+-----------------------+----------------+------------------+--------------------------------+
  //        |""".stripMargin
  //
  //    val ignoreConfiguration = TestConfiguration(V2_3 -> V3_2, Planners.all, Runtimes.all ) + Configs.AllRulePlanners
  //    executeWith(expectedToSucceed, query,planComparisonStrategy = ComparePlansWithAssertion(_ should matchPlan(expectedPlan), expectPlansToFail = ignoreConfiguration))
  //>>>>>>> More ported test before bug fixig.
  //  }

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
