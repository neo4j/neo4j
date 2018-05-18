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

import scala.collection.Map

class HintAcceptanceTest
  extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should use a simple hint") {
    val query = "MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c"
    executeWith(Configs.AllExceptSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeHashJoin"),
        expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should not plan multiple joins for one hint") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.CommunityInterpreted - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should useOperators("NodeOuterHashJoin")
        p should not(useOperators("NodeHashJoin"))
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1))
  }

  test("should solve join hint on 1 variable with join on more, if possible") {
    val query =
      """MATCH (pA:Person),(pB:Person) WITH pA, pB
        |
        |OPTIONAL MATCH
        |  (pA)<-[:HAS_CREATOR]-(pB)
        |USING JOIN ON pB
        |RETURN *""".stripMargin

    executeWith(Configs.CommunityInterpreted - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should useOperators("NodeOuterHashJoin")
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1))
  }

  test("should do index seek instead of index scan with explicit index seek hint") {
    graph.createIndex("A", "prop")
    graph.createIndex("B", "prop")

    graph.inTx {
      createLabeledNode(Map("prop" -> 42), "A")
      createLabeledNode(Map("prop" -> 1337), "B")
    }

    // At the time of writing this test fails with generic index hints:
    // USING INDEX a:A(prop)
    // USING INDEX b:B(prop)
    val query = """EXPLAIN
                  |LOAD CSV WITH HEADERS FROM 'file:///dummy.csv' AS row
                  |MATCH (a:A), (b:B)
                  |USING INDEX SEEK a:A(prop)
                  |USING INDEX SEEK b:B(prop)
                  |WHERE a.prop = row.propA AND b.prop = row.propB
                  |RETURN a.prop, b.prop
                """.stripMargin

    // TODO: Once 3.2 comes out with this feature added, we should change the following line to not exclude 3.2
    executeWith(Configs.CommunityInterpreted - Configs.AllRulePlanners - Configs.Cost2_3 - Configs.Cost3_1 - Configs.Cost3_2, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should useOperatorTimes("NodeIndexSeek", 2)
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1))
  }
}
