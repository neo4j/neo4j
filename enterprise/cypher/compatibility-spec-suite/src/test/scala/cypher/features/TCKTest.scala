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

import java.nio.file.FileSystems
import java.util.Collection

import cypher.features.ScenarioTestHelper._
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{AfterEach, DynamicTest, Test, TestFactory}
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

class TCKTest {

  // these two should be empty on commit!
  val featureToRun = ""
  val scenarioToRun = ""

  val allScenarios = CypherTCK.allTckScenarios
  val scenarios: Seq[Scenario] = {
    if (featureToRun.nonEmpty) {
      val filteredFeature = allScenarios.filter(s => s.featureName == featureToRun)
      if (scenarioToRun.nonEmpty) {
        filteredFeature.filter(s => s.name == scenarioToRun)
      } else
        filteredFeature
    } else if (scenarioToRun.nonEmpty) {
      allScenarios.filter(s => s.name == scenarioToRun)
    } else
      allScenarios
  }

  @AfterEach
  def tearDown(): Unit = {
    //TODO: This method can be removed with new release of TCK (1.0.0-M10)
    FileSystems.getFileSystem(CypherTCK.getClass.getResource(CypherTCK.featuresPath).toURI).close()
  }

  @TestFactory
  def runTCKTestsDefault(): Collection[DynamicTest] = {
    createTests(scenarios, DefaultTestConfig)
  }

  @TestFactory
  def runTCKTestsCostSlotted(): Collection[DynamicTest] = {
    createTests(scenarios, CostSlottedTestConfig)
  }

  //  Morsel engine is not complete and executes tests very slowly
  // eg. MorselExecutionContext.createClone is not implemented
  //  @TestFactory
  //  def runTCKTestsCostMorsel(): Collection[DynamicTest] = {
  // TODO: once Morsel is complete, generate blacklist with: generateBlacklistTCKTestCostMorsel further down
  //    createTests(scenarios, CostMorselTestConfig)
  //  }

  @TestFactory
  def runTCKTestsCostCompiled(): Collection[DynamicTest] = {
    createTests(scenarios, CostCompiledTestConfig)
  }

  @TestFactory
  def runTCKTestsCost(): Collection[DynamicTest] = {
    createTests(scenarios, CostTestConfig)
  }

  @TestFactory
  def runTCKTestsCompatibility33(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility33TestConfig)
  }

  @TestFactory
  def runTCKTestsCompatibility31(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility31TestConfig)
  }

  @TestFactory
  def runTCKTestsCompatibility23(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility23TestConfig)
  }

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }

  /*
  All methods for generating blacklists. Comment them out for commit
   */
/*
  @Test
  def generateBlacklistTCKTestDefault(): Unit = {
    printComputedBlacklist(scenarios, DefaultTestConfig)
    fail("Do not forget to comment this method out")
  }

  @Test
  def generateBlacklistTCKTestCostSlotted(): Unit = {
    printComputedBlacklist(scenarios, CostSlottedTestConfig)
    fail("Do not forget to comment this method out")
  }

  //  Morsel engine is not complete and executes tests very slowly
  // eg. MorselExecutionContext.createClone is not implemented
  //  @Test
  //  def generateBlacklistTCKTestCostMorsel(): Unit = {
  //    printComputedBlacklist(scenarios, CostMorselTestConfig)
  //    fail("Do not forget to comment this method out")
  //  }

  @Test
  def generateBlacklistTCKTestCostCompiled(): Unit = {
    printComputedBlacklist(scenarios, CostCompiledTestConfig)
    fail("Do not forget to comment this method out")
  }

  @Test
  def generateBlacklistTCKTestCost(): Unit = {
    printComputedBlacklist(scenarios, CostTestConfig)
    fail("Do not forget to comment this method out")
  }

  @Test
  def generateBlacklistTCKTestCompatibility33(): Unit = {
    printComputedBlacklist(scenarios, Compatibility33TestConfig)
    fail("Do not forget to comment this method out")
  }

  @Test
  def generateBlacklistTCKTestCompatibility31(): Unit = {
    printComputedBlacklist(scenarios, Compatibility31TestConfig)
    fail("Do not forget to comment this method out")
  }

  @Test
  def generateBlacklistTCKTestCompatibility23(): Unit = {
    printComputedBlacklist(scenarios, Compatibility23TestConfig)
    fail("Do not forget to comment this method out")
  }
*/
}
