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

import scala.collection.Map

class HintAcceptanceTest
    extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should use a simple hint") {
    val query = "MATCH (a)--(b)--(c) USING JOIN ON b RETURN a,b,c"
    executeWith(Configs.All, query, planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeHashJoin"), expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should not plan multiple joins for one hint - left outer join") {
    val a = createLabeledNode(Map[String, Any]("name" -> "a"), "A")
    for(i <- 0 until 10) {
      val b = createLabeledNode(Map[String, Any]("name" -> s"${i}b"), "B")
      relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.Interpreted + Configs.Version3_3 - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
      p should useOperators("NodeLeftOuterHashJoin")
      p should not(useOperators("NodeHashJoin"))
    }, expectPlansToFail = Configs.OldAndRule))
  }

  test("should not plan multiple joins for one hint - right outer join") {
    val b = createLabeledNode(Map[String, Any]("name" -> "b"), "B")
    for(i <- 0 until 10) {
      val a = createLabeledNode(Map[String, Any]("name" -> s"${i}a"), "A")
      if(i == 0) relate(a, b)
    }

    val query = """MATCH (a:A)
                  |OPTIONAL MATCH (a)-->(b:B)
                  |USING JOIN ON a
                  |RETURN a.name, b.name""".stripMargin

    executeWith(Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1, query, planComparisonStrategy = ComparePlansWithAssertion((p) => {
      p should useOperators("NodeRightOuterHashJoin")
      p should not(useOperators("NodeHashJoin"))
    }, expectPlansToFail = Configs.AllRulePlanners + Configs.BackwardsCompatibility))
  }

  test("should solve join hint on 1 variable with join on more, if possible") {
    val query =
      """MATCH (pA:Person),(pB:Person) WITH pA, pB
        |
        |OPTIONAL MATCH
        |  (pA)<-[:HAS_CREATOR]-(pB)
        |USING JOIN ON pB
        |RETURN *""".stripMargin

    // TODO: Once 3.3 comes out with the same bugfix, we should change the following lines to not exclude 3.3
    val cost3_3 = TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)
    executeWith(Configs.Interpreted - Configs.Cost2_3 - Configs.Cost3_1, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should useOperators("NodeRightOuterHashJoin")
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1 + cost3_3))
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

    // TODO: Once 3.2 comes out with this feature added, we should change the following line to not exclude 3.3
    val cost3_3 = TestScenario(Versions.V3_3, Planners.Cost, Runtimes.Default)
    executeWith(Configs.Interpreted - Configs.AllRulePlanners - Configs.Cost2_3 - Configs.Cost3_1 - cost3_3, query,
      planComparisonStrategy = ComparePlansWithAssertion((p) => {
        p should useOperatorTimes("NodeIndexSeek", 2)
      }, expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3 + Configs.Cost3_1))
  }
}
