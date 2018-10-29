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
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

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
      Configs.InterpretedAndSlotted,
      "MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 RETURN count(*)",
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.aPlan("Apply").withRHS(aPlan("NodeIndexSeek"))
        planDescription should not(includeSomewhere.aPlan("ValueHashJoin") or
          includeSomewhere.aPlan("CartesianProduct") or
          includeSomewhere.aPlan("NodeByLabelScan") or
          includeSomewhere.aPlan("Filter"))
      }, expectPlansToFail = Configs.RulePlanner + Configs.Cost2_3))

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
    val result = executeWith(Configs.InterpretedAndSlotted,
      "MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 OPTIONAL MATCH (a)-[:T]->() RETURN count(*)",
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription should includeSomewhere.aPlan("Apply").withRHS(aPlan("NodeIndexSeek"))
        planDescription should not(includeSomewhere.aPlan("ValueHashJoin") or
          includeSomewhere.aPlan("CartesianProduct") or
          includeSomewhere.aPlan("NodeByLabelScan") or
          includeSomewhere.aPlan("Filter"))
      }, expectPlansToFail = Configs.RulePlanner + Configs.Cost2_3))

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
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f")), expectPlansToFail = Configs.RulePlanner))

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
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f")), expectPlansToFail = Configs.RulePlanner + Configs.Cost2_3))
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
      planComparisonStrategy = ComparePlansWithAssertion( _ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("f")), expectPlansToFail = Configs.RulePlanner + Configs.Cost2_3))
    result.columnAs[Node]("f").toSet should equal(Set(nodes(122), nodes(123)))
  }

  test("should be able to plan index use for inequality") {
    val aNodes = (0 to 125).map(i => createLabeledNode(Map("prop" -> i), "Foo"))
    val bNode = createLabeledNode(Map("prop2" -> 122), "Bar")
    graph.createIndex("Foo", "prop")
    val query =
      """ MATCH (a:Foo), (b:Bar) WHERE a.prop > b.prop2
        | RETURN a
      """.stripMargin

    val result = executeWith(Configs.All - Configs.Compiled, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgument(":Foo(prop) > b.prop2"),
        expectPlansToFail = Configs.Version2_3 + Configs.Version3_1 + Configs.Version3_4))

    result.columnAs[Node]("a").toList should equal(List(aNodes(123), aNodes(124), aNodes(125)))
  }

  test("should be able to plan index use for starts with") {
    val aNodes = (0 to 125).map(i => createLabeledNode(Map("prop" -> s"${i}string"), "Foo"))
    val bNode = createLabeledNode(Map("prop2" -> "12"), "Bar")
    graph.createIndex("Foo", "prop")
    val query =
      """ MATCH (a:Foo), (b:Bar) WHERE a.prop STARTS WITH b.prop2
        | RETURN a
      """.stripMargin

    val result = executeWith(Configs.All - Configs.Compiled, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgument(":Foo(prop STARTS WITH b.prop2)"),
        expectPlansToFail = Configs.Version2_3 + Configs.Version3_1 + Configs.Version3_4))

    result.columnAs[Node]("a").toSet should equal(Set(aNodes(12), aNodes(120), aNodes(121), aNodes(122), aNodes(123), aNodes(124), aNodes(125)))
  }

  // TODO: Not sure why this is not solved using indexes. Come back to this once we have good index support for CONTAINS and ENDS WITH
  ignore("should be able to plan index use for CONTAINS") {
    val aNodes = (0 to 1250).map(i => createLabeledNode(Map("prop" -> s"prefix${i}suffix"), "Foo"))
    val bNode = createLabeledNode(Map("prop2" -> "12"), "Bar")
    graph.createIndex("Foo", "prop")
    val query =
      """ MATCH (a:Foo), (b:Bar) WHERE a.prop CONTAINS b.prop2
        | RETURN a
      """.stripMargin

    val result = executeWith(Configs.All - Configs.Compiled, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgument(":Foo(prop CONTAINS b.prop2)"),
        expectPlansToFail = Configs.Version2_3 + Configs.Version3_1 + Configs.Version3_4))

    result.columnAs[Node]("a").toSet should equal(Set(aNodes(12), aNodes(120), aNodes(121), aNodes(122), aNodes(123), aNodes(124), aNodes(125)))
  }

  ignore("should be able to plan index use for ENDS WITH") {
    val aNodes = (0 to 1250).map(i => createLabeledNode(Map("prop" -> s"prefix${i}"), "Foo"))
    val bNode = createLabeledNode(Map("prop2" -> "12"), "Bar")
    graph.createIndex("Foo", "prop")
    val query =
      """ MATCH (a:Foo), (b:Bar) WHERE a.prop ENDS WITH b.prop2
        | RETURN a
      """.stripMargin

    val result = executeWith(Configs.All - Configs.Compiled, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgument(":FOO(prop ENDS WITH b.prop2)"),
        expectPlansToFail = Configs.Version2_3 + Configs.Version3_1 + Configs.Version3_4))

    result.columnAs[Node]("a").toSet should equal(Set(aNodes(12), aNodes(120), aNodes(121), aNodes(122), aNodes(123), aNodes(124), aNodes(125)))
  }

  test("should be able to plan index use for spatial index queries") {
    // Given
    graph.execute(
      """CREATE (:Bar {location: point({ x:0, y:100 })})
        |WITH 1 as whyOwhy
        |UNWIND range(1,200) AS y
        |CREATE (:Foo {location: point({ x:0, y:y })})
      """.stripMargin)
    graph.createIndex("Foo", "location")
    val query =
      """ MATCH (a:Foo), (b:Bar) WHERE distance(a.location, b.location) < 2
        | RETURN id(a)
      """.stripMargin

    // Matches the word "distance" anywhere in the argument string
    val distanceRegex = ".*distance.*".r
    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version3_1 - Configs.Version2_3, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.aPlan("NodeIndexSeekByRange").containingArgumentRegex(distanceRegex),
        expectPlansToFail = Configs.Version2_3 + Configs.Version3_1 + Configs.Version3_4))

    result.columnAs[Node]("id(a)").toList should equal(List(99, 100, 101))
  }

}
