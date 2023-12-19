/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package cypher.features

import java.util

import cypher.features.ScenarioTestHelper.{createTests, printComputedBlacklist}
import org.junit.Ignore
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.opencypher.tools.tck.api.Scenario

@RunWith(classOf[JUnitPlatform])
class Compatibility31TCKTests extends BaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCompatibility31(): util.Collection[DynamicTest] = {
    val filteredScenarios = scenarios.filterNot(testsWithProblems)
    createTests(filteredScenarios, Compatibility31TestConfig)
  }

  //TODO: Fix Schroedinger's test cases in TCK or find way to handle here
  /*
    These tests run with parameters multiple times under the same name.
    The run with the first parameter will succeed and the next ones fail because they try to put e.g. date() inside arrays, which is not possible for
    non-property types in 3.1.
    So they both succeed AND fail -> Thus we cannot "just" blacklist them and need to ignore them completely
   */
  def testsWithProblems(scenario: Scenario): Boolean = {
    (scenario.name.equals("Should store date") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store local time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store local date time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store date time") && scenario.featureName.equals("TemporalCreateAcceptance")) ||
      (scenario.name.equals("Should store duration") && scenario.featureName.equals("TemporalCreateAcceptance"))
  }

  @Ignore
  def generateBlacklistCompatibility31(): Unit = {
    printComputedBlacklist(scenarios, Compatibility31TestConfig)
    fail("Do not forget to add @ignore to this method")
  }
}
