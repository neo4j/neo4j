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
package cypher.features

import java.io.File
import java.net.URI
import java.util.Collection

import cypher.features.ScenarioTestHelper._
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{DynamicTest, Test, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

class AcceptanceTests extends BaseFeatureTest {

  // these two should be empty on commit!
  override val featureToRun = ""
  override val scenarioToRun = ""

  val featuresURI: URI = getClass.getResource("/cypher/features").toURI

  override val scenarios: Seq[Scenario] = {
    val all = CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios)
    filterScenarios(all)
  }

  @TestFactory
  override def runCompatibility23(): Collection[DynamicTest] = {
    //TODO: Investigate flakiness
    def isFlakyOn23(scenario: Scenario): Boolean = {
      scenario.name == "STARTS WITH should handle null prefix" && scenario.featureName == "IndexAcceptance"
    }
    createTests(scenarios.filterNot(isFlakyOn23), Compatibility23TestConfig)
  }
}
