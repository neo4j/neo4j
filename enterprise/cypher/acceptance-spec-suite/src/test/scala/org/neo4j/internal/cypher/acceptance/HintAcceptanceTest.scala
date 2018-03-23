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
}
