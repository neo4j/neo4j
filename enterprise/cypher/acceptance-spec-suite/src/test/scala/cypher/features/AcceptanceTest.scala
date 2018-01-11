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
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

class AcceptanceTest {

  val featureToRun = ""
  val scenarioToRun = ""

  val featuresURI: URI = getClass.getResource("/cypher/features").toURI

  val scenarios: Seq[Scenario] = {
    val all = CypherTCK.parseFilesystemFeatures(new File(featuresURI)).flatMap(_.scenarios)
    if (featureToRun.nonEmpty) {
      val filteredFeature = all.filter(s => s.featureName == featureToRun)
      if (scenarioToRun.nonEmpty) {
        filteredFeature.filter(s => s.name == scenarioToRun)
      } else
        filteredFeature
    } else if (scenarioToRun.nonEmpty) {
      all.filter(s => s.name == scenarioToRun)
    } else
      all
  }

  @TestFactory
  def runAcceptanceTestsDefault(): Collection[DynamicTest] = {
    createTests(scenarios, DefaultTestConfig)
  }

  @TestFactory
  def runAcceptanceTestsCostSlotted(): Collection[DynamicTest] = {
    createTests(scenarios, CostSlottedTestConfig)
  }

  //  Morsel engine is not complete and executes tests very slowly
  // eg. MorselExecutionContext.createClone is not implemented
  //  @TestFactory
  //  def runAcceptanceTestsCostMorsel() = {
  // TODO: once Morsel is complete, generate blacklist with: printComputedBlacklist(scenarios, CostMorselTestConfig)
  //    createTests(scenarios, CostMorselTestConfig)
  //  }

  @TestFactory
  def runAcceptanceTestsCostCompiled(): Collection[DynamicTest] = {
    createTests(scenarios, CostCompiledTestConfig)
  }

  @TestFactory
  def runAcceptanceTestsCost(): Collection[DynamicTest] = {
    createTests(scenarios, CostTestConfig)
  }

  @TestFactory
  def runAcceptanceTestsCompatibility33(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility33TestConfig)
  }

  @TestFactory
  def runAcceptanceTestsCompatibility31(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility31TestConfig)
  }

  @TestFactory
  def runAcceptanceTestsCompatibility23(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility23TestConfig)
  }
}
