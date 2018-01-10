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
package cypher.features

import cypher.features.ScenarioTestHelper._
import org.junit.jupiter.api.TestFactory
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings._
import org.opencypher.tools.tck.api.CypherTCK

class TCKTest {

//  val tckSemanticFailures = Set(
//    // Neo4j fails at runtime, should fail at compile time
//    "Failing on merging relationship with null property",
//    "Failing on merging node with null property",
//    "Failing when setting a list of maps as a property",
//
//    // YIELD -
//    "In-query call to procedure that takes no arguments and yields no results",
//    "In-query call to procedure with explicit arguments that drops all result fields",
//
//    // Integer/Float conversion
//    "Standalone call to procedure with argument of type INTEGER accepts value of type FLOAT",
//    "In-query call to procedure with argument of type INTEGER accepts value of type FLOAT",
//
//    // Different error type in Neo4j
//    "Deleting connected nodes",
//    "Standalone call to unknown procedure should fail",
//    "In-query call to unknown procedure should fail"
//  )

  val scenarios = CypherTCK.allTckScenarios
     // .filter(s => s.name == "Count nodes")

  @TestFactory
  def runTCKTestsDefault() = {
    val blacklist = "default.txt"
    val blacklistedScenarioNames = parseBlacklist(blacklist)
    runAndCheckBlacklistedTests(scenarios, blacklistedScenarioNames)
    createTests(scenarios, blacklistedScenarioNames)
  }

  @TestFactory
  def runTCKTestsCostCompiled() = {
    val config = Map[Setting[_],String](
      cypher_planner -> "COST",
      cypher_runtime -> "COMPILED",
      cypher_hints_error -> "true")
    val blacklist = "cost-compiled.txt"
    val blacklistedScenarioNames = parseBlacklist(blacklist)

    runAndCheckBlacklistedTests(scenarios, blacklistedScenarioNames, config)
    createTests(scenarios, blacklistedScenarioNames, config)
  }



}
