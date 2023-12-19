/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

  test("should not crash on any() on rhs of NodeHashJoin") {
    graph.execute("CREATE (a: A {prop: 1})-[:X]->(b {prop: 2})-[:X]->(c {prop: 1, list: [1,3,4]})")

    val query =
      """MATCH (a: A)-[:X]->(b)-[:X]->(c)
        |USING JOIN ON b
        |WHERE any(x IN c.list WHERE x % 2 = 1)
        |RETURN b.prop""".stripMargin

    val result = innerExecuteDeprecated(query)
    result.executionPlanDescription() should useOperators("NodeHashJoin")
    result.toList should be(List(Map("b.prop" -> 2)))
  }

  test("should get result for query with complex joins using two MATCH clauses") {
    // GIVEN

    // (f00)-[:right]->(f01)-[:right]->(f02)
    //   |              |                |
    // [:bottom]     [:bottom]       [:bottom]
    //   |              |                |
    //   v              v                v
    // (f10)-[:right]->(f11)-[:right]->(f12)
    //   |              |                |
    // [:bottom]     [:bottom]       [:bottom]
    //   |              |                |
    //   v              v                v
    // (f20)-[:right]->(f21)-[:right]->(f22)
    innerExecuteDeprecated(
      """
        |CREATE
        |(f00:Field {name: "f00"})-[:right{name: "f00f01"}]->(f01:Field {name: "f01"})-[:right{name: "f01f02"}]->(f02:Field {name: "f02"}),
        |(f10:Field {name: "f10"})-[:right{name: "f10f11"}]->(f11:Field {name: "f11"})-[:right{name: "f11f12"}]->(f12:Field {name: "f12"}),
        |(f20:Field {name: "f20"})-[:right{name: "f20f21"}]->(f21:Field {name: "f21"})-[:right{name: "f21f22"}]->(f22:Field {name: "f22"}),
        |(f00)-[:bottom{name: "f00f10"}]->(f10)-[:bottom{name: "f10f20"}]->(f20),
        |(f01)-[:bottom{name: "f01f11"}]->(f11)-[:bottom{name: "f11f21"}]->(f21),
        |(f02)-[:bottom{name: "f02f12"}]->(f12)-[:bottom{name: "f12f22"}]->(f22),
        |(:Board {name: "b"})
      """.stripMargin)

    // The board points to all fields
    innerExecuteDeprecated("MATCH (f:Field), (b:Board) CREATE (b)-[:fields {name: 'b'+f.name}]->(f)")

    // Search for 3x3 matrix matching the setup
    // LIMIT is needed to trigger the (previously) faulty plan
    val query =
    """
      |MATCH (b:Board)
      |WITH b, 1 AS ignore
      |MATCH
      |(b)-[b_fields_0_field11:fields]->(field11:Field),
      |(b)-[b_fields_1_field12:fields]->(field12:Field),
      |(b)-[b_fields_2_field13:fields]->(field13:Field),
      |(b)-[b_fields_3_field21:fields]->(field21:Field),
      |(b)-[b_fields_4_field22:fields]->(field22:Field),
      |(b)-[b_fields_5_field23:fields]->(field23:Field),
      |(b)-[b_fields_6_field31:fields]->(field31:Field),
      |(b)-[b_fields_7_field32:fields]->(field32:Field),
      |(b)-[b_fields_8_field33:fields]->(field33:Field),
      |(field11)-[field11_right_0_field12:right]->(field12), (field11)-[field11_bottom_1_field21:bottom]->(field21),
      |(field12)-[field12_right_0_field13:right]->(field13), (field12)-[field12_bottom_1_field22:bottom]->(field22),
      |(field13)-[field13_bottom_0_field23:bottom]->(field23),
      |(field21)-[field21_right_0_field22:right]->(field22), (field21)-[field21_bottom_1_field31:bottom]->(field31),
      |(field22)-[field22_right_0_field23:right]->(field23), (field22)-[field22_bottom_1_field32:bottom]->(field32),
      |(field23)-[field23_bottom_0_field33:bottom]->(field33),
      |(field31)-[field31_right_0_field32:right]->(field32),
      |(field32)-[field32_right_0_field33:right]->(field33)
      |WHERE NOT id(field11) = id(field12) AND NOT id(field11) = id(field13) AND NOT id(field11) = id(field21) AND NOT id(field11) = id(field22)
      |   AND NOT id(field11) = id(field23) AND NOT id(field11) = id(field31) AND NOT id(field11) = id(field32) AND NOT id(field11) = id(field33)
      | AND NOT id(field12) = id(field13) AND NOT id(field12) = id(field21) AND NOT id(field12) = id(field22) AND NOT id(field12) = id(field23)
      |   AND NOT id(field12) = id(field31) AND NOT id(field12) = id(field32) AND NOT id(field12) = id(field33)
      | AND NOT id(field13) = id(field21) AND NOT id(field13) = id(field22) AND NOT id(field13) = id(field23) AND NOT id(field13) = id(field31)
      |   AND NOT id(field13) = id(field32) AND NOT id(field13) = id(field33)
      | AND NOT id(field21) = id(field22) AND NOT id(field21) = id(field23) AND NOT id(field21) = id(field31) AND NOT id(field21) = id(field32)
      |   AND NOT id(field21) = id(field33)
      | AND NOT id(field22) = id(field23) AND NOT id(field22) = id(field31) AND NOT id(field22) = id(field32) AND NOT id(field22) = id(field33)
      | AND NOT id(field23) = id(field31) AND NOT id(field23) = id(field32) AND NOT id(field23) = id(field33)
      | AND NOT id(field31) = id(field32) AND NOT id(field31) = id(field33)
      | AND NOT id(field32) = id(field33)
      |RETURN
      |b_fields_0_field11.name AS b_field11, b_fields_1_field12.name AS b_field12, b_fields_2_field13.name AS b_field13,
      |b_fields_3_field21.name AS b_field21, b_fields_4_field22.name AS b_field22, b_fields_5_field23.name AS b_field23,
      |b_fields_6_field31.name AS b_field31, b_fields_7_field32.name AS b_field32, b_fields_8_field33.name AS b_field33
      |LIMIT 1
      """.stripMargin

    // WHEN
    val result = executeWith(Configs.Interpreted, query)

    // THEN
    result.toComparableResult should be(Seq(Map(
      "b_field11" -> "bf00", "b_field12" -> "bf01", "b_field13" -> "bf02",
      "b_field21" -> "bf10", "b_field22" -> "bf11", "b_field23" -> "bf12",
      "b_field31" -> "bf20", "b_field32" -> "bf21", "b_field33" -> "bf22"
    )))
  }
}
