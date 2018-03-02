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

  test("optional match join should not crash") {
    val query =
      """MATCH (a:A)-->(b:B)-->(c:C)
        |OPTIONAL MATCH (h)<--(g:G)<--(c)
        |USING JOIN ON c
        |RETURN a,b,c,g,h""".stripMargin
    graph.execute(query) // should not crash
  }
}
