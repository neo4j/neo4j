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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}

class JoinAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  private val expectedToSucceed = Configs.Interpreted
  private val expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3

  test("find friends of others") {
    // given
    createLabeledNode(Map("id" -> 1), "A")
    val a = createLabeledNode(Map("id" -> 2), "A")
    val b = createLabeledNode(Map("id" -> 2), "B")
    createLabeledNode(Map("id" -> 3), "B")

    // when
    executeWith(expectedToSucceed, "MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("ValueHashJoin"), expectPlansToFail))
  }

  test("should reverse direction if lhs is much larger than rhs") {
    // given
    (0 to 1000) foreach { x =>
      createLabeledNode(Map("id" -> x), "A")
    }

    (0 to 10) foreach { x =>
      createLabeledNode(Map("id" -> x), "B")
    }

    // when
    executeWith(expectedToSucceed, "MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("ValueHashJoin"), expectPlansToFail))
  }

  test("should handle node left outer hash join") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    createLabeledNode(Map[String, Any]("name" -> "a2"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      if(i != 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    val expectSucceed = Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1
    executeWith(expectSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeLeftOuterHashJoin"), expectPlansToFail))
  }

  test("should handle node right outer hash join") {
    val b = createLabeledNode(Map[String, Any]("name" -> "b"), "B")
    createLabeledNode(Map[String, Any]("name" -> "b2"), "B")
    for(i <- 0 until 10) {
      val a = createLabeledNode(Map[String, Any]("name" -> s"${i}a"), "A")
      if(i == 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    val expectSucceed = Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1
    executeWith(expectSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeRightOuterHashJoin"), Configs.AllRulePlanners + Configs.BackwardsCompatibility))
  }

  test("should handle node left outer hash join with different types for the node variable") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    createLabeledNode(Map[String, Any]("name" -> "a2"), "A")
    for(i <- 0 until 50) { // This number is sensitive in that it has to exceed the cardinality estimation of the UNWIND
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      if(i != 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |UNWIND [a] as refA
                  |OPTIONAL MATCH (refA)-->(b:B)
                  |USING JOIN ON refA
                  |RETURN a.name, b.name""".stripMargin

    val expectSucceed = Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1
    executeWith(expectSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeLeftOuterHashJoin"), expectPlansToFail))
  }

  test("should handle node right outer hash join with different types for the node variable") {
    val b = createLabeledNode(Map[String, Any]("name" -> "b"), "B")
    createLabeledNode(Map[String, Any]("name" -> "b2"), "B")
    for(i <- 0 until 10) {
      val a = createLabeledNode(Map[String, Any]("name" -> s"${i}a"), "A")
      if(i == 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |UNWIND [a] as refA
                  |OPTIONAL MATCH (refA)-->(b:B)
                  |USING JOIN ON refA
                  |RETURN a.name, b.name""".stripMargin

    val expectSucceed = Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1
    val result = executeWith(expectSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeRightOuterHashJoin"), Configs.AllRulePlanners + Configs.BackwardsCompatibility))
  }

  test("optional match join should not crash") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)
        |OPTIONAL MATCH (h)<--(g:G)<--(c)
        |USING JOIN ON c
        |RETURN a,b,c,g,h""".stripMargin
    graph.execute(query) // should not crash
  }

test("larger optional match join should not crash") {
    val query =
      """MATCH (b:B)-->(c:C)
        |OPTIONAL MATCH (c)<--(d:D)
        |USING JOIN ON c
        |OPTIONAL MATCH (g:G)<--(c)
        |USING JOIN ON c
        |RETURN b,c,d,g""".stripMargin
    graph.execute(query) // should not crash
  }

  test("order in which join hints are solved should not matter") {
    val query =
      """MATCH (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)-[:X]->(e)
        |USING JOIN ON b
        |USING JOIN ON c
        |USING JOIN ON d
        |WHERE a.prop = e.prop
        |RETURN b, d""".stripMargin
    graph.execute(query) // should not crash
  }
}