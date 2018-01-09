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

import java.io.File

import cypher.features.ScenarioTestHelper.{createTests, parseBlacklist}
import org.junit.jupiter.api.TestFactory
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings.{cypher_hints_error, cypher_planner, cypher_runtime}
import org.opencypher.tools.tck.api.CypherTCK

class AcceptanceTest {

  val featuresURI = getClass.getResource("/cypher/features").toURI

  val acceptanceSemanticFailures = Set[String](
    // Different error type in Neo4j
    "Standalone call to unknown procedure should fail",
    "In-query call to unknown procedure should fail"
  )

  val scenarios = CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios).
    filterNot(scenario => acceptanceSemanticFailures.contains(scenario.name))

  @TestFactory
  def runAcceptanceTestsDefault() = {
    createTests(scenarios)
  }

  @TestFactory
  def runAcceptanceTestsCostCompiled() = {
    val config = Map[Setting[_],String](
      cypher_planner -> "COST",
      cypher_runtime -> "COMPILED",
      cypher_hints_error -> "true")
    val blacklist = "cost-compiled.txt"

    createTests(scenarios, parseBlacklist(blacklist), config)
  }

}
