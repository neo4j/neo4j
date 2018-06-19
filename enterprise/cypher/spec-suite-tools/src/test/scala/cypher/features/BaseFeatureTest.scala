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
package cypher.features

import java.util.Collection

import cypher.features.ScenarioTestHelper._
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{Disabled, DynamicTest, Test, TestFactory}
import org.opencypher.tools.tck.api.Scenario

abstract class BaseFeatureTest {
  // these two should be empty on commit!
  val featureToRun = ""
  val scenarioToRun = ""

  val scenarios: Seq[Scenario]

  def filterScenarios(allScenarios: Seq[Scenario]): Seq[Scenario] = {
    if (featureToRun.nonEmpty) {
      val filteredFeature = allScenarios.filter(s => s.featureName.contains(featureToRun))
      if (scenarioToRun.nonEmpty) {
        filteredFeature.filter(s => s.name.contains(scenarioToRun))
      } else
        filteredFeature
    } else if (scenarioToRun.nonEmpty) {
      allScenarios.filter(s => s.name.contains(scenarioToRun))
    } else
      allScenarios
  }

  @TestFactory
  def runDefault(): Collection[DynamicTest] = {
    createTests(scenarios, DefaultTestConfig)
  }

  @TestFactory
  def runCostSlotted(): Collection[DynamicTest] = {
    createTests(scenarios, CostSlottedTestConfig)
  }

  @TestFactory
  def runCostMorsel(): Collection[DynamicTest] = {
     createTests(scenarios, CostMorselTestConfig)
  }

  @TestFactory
  def runCostCompiled(): Collection[DynamicTest] = {
    createTests(scenarios, CostCompiledTestConfig)
  }

  @TestFactory
  def runCostInterpreted(): Collection[DynamicTest] = {
    createTests(scenarios, CostInterpretedTestConfig)
  }

  @TestFactory
  def runCompatibility33(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility33TestConfig)
  }

  @TestFactory
  def runCompatibility31(): Collection[DynamicTest] = {
    createTests(scenarios, Compatibility31TestConfig)
  }

  @TestFactory
  def runCompatibility23(): Collection[DynamicTest] = {
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
  All methods for generating blacklists. Ignore them for commit
   */

  @Disabled
  def generateBlacklistDefault(): Unit = {
    printComputedBlacklist(scenarios, DefaultTestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCostSlotted(): Unit = {
    printComputedBlacklist(scenarios, CostSlottedTestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistTCKTestCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselTestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCostCompiled(): Unit = {
    printComputedBlacklist(scenarios, CostCompiledTestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCostInterpreted(): Unit = {
    printComputedBlacklist(scenarios, CostInterpretedTestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCompatibility33(): Unit = {
    printComputedBlacklist(scenarios, Compatibility33TestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCompatibility31(): Unit = {
    printComputedBlacklist(scenarios, Compatibility31TestConfig)
    fail("Do not forget to add @Disabled to this method")
  }

  @Disabled
  def generateBlacklistCompatibility23(): Unit = {
    printComputedBlacklist(scenarios, Compatibility23TestConfig)
    fail("Do not forget to add @Disabled to this method")
  }
}
