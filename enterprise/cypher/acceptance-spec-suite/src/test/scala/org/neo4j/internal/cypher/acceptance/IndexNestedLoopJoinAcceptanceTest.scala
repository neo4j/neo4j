/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class IndexNestedLoopJoinAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  test("test that index seek is planned on the RHS using information from the LHS") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWith(
      Configs.Interpreted,
      "MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 RETURN count(*)",
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should useOperators("Apply", "NodeIndexSeek")
        planDescription should not(useOperators("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))

    result.toList should equal(List(Map("count(*)" -> 3)))
  }

  test("index seek planned in the presence of optional matches") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWith(Configs.Interpreted,
      "MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 OPTIONAL MATCH (a)-[:T]->() RETURN count(*)",
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should useOperators("Apply", "NodeIndexSeek")
        planDescription should not(useOperators("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))

    result.toList should equal(List(Map("count(*)" -> 3)))
  }

  test("should use index on variable defined from literal map") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    graph.createIndex("Foo", "id")
    val query =
      """
        | WITH [{id: 123}, {id: 122}] AS rows
        | UNWIND rows AS row
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=row.id
        | RETURN f
      """.stripMargin

    val result = executeWith(Configs.All - Configs.Version2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"), expectPlansToFail = Configs.AllRulePlanners ))

    result.columnAs[Node]("f").toSet should equal(Set(nodes(122),nodes(123)))
  }

  test("should use index on other node property value where there is no incoming horizon") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    val n1=createLabeledNode(Map("id"->122),"Bar")
    val n2=createLabeledNode(Map("id"->123),"Bar")
    graph.createIndex("Foo", "id")
    val query =
      """
        | MATCH (b:Bar) WHERE b.id = 123
        | MATCH (f:Foo) WHERE f.id = b.id
        | RETURN f
      """.stripMargin
    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"), expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))
    result.columnAs[Node]("f").toList should equal(List(nodes(123)))
  }

  test("should use index on other node property value where there is an incoming horizon") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    val n1=createLabeledNode(Map("id"->122),"Bar")
    val n2=createLabeledNode(Map("id"->123),"Bar")
    graph.createIndex("Foo", "id")
    val query =
      """
        | WITH [122, 123] AS rows
        | UNWIND rows AS row
        | MATCH (b:Bar) WHERE b.id = row
        | MATCH (f:Foo) WHERE f.id = b.id
        | RETURN f
      """.stripMargin
    val result = executeWith(Configs.All, query,
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f"), expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3))
    result.columnAs[Node]("f").toSet should equal(Set(nodes(122), nodes(123)))
  }
}
