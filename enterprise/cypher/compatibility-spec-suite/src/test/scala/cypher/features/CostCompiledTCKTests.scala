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

@RunWith(classOf[JUnitPlatform])
class CostCompiledTCKTests extends BaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  @TestFactory
  def runCostCompiled(): util.Collection[DynamicTest] = {
    createTests(scenarios, CostCompiledTestConfig)
  }

  @Ignore
  def generateBlacklistCostCompiled(): Unit = {
    printComputedBlacklist(scenarios, CostCompiledTestConfig)
    fail("Do not forget to add @ignore to this method")
  }
}
