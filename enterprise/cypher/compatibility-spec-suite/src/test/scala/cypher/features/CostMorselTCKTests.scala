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

import cypher.features.ScenarioTestHelper.printComputedBlacklist
import org.junit.Ignore
import org.junit.jupiter.api.Assertions.fail

class CostMorselTCKTests extends BaseTCKTests {

  // If you want to only run a specific feature or scenario, go to the BaseTCKTests

  //  Morsel engine is not complete and executes tests very slowly
  // eg. MorselExecutionContext.createClone is not implemented
  //  @TestFactory
  //  def runCostMorsel(): Collection[DynamicTest] = {
  // TODO: once Morsel is complete, generate blacklist with: generateBlacklistCostMorsel further down
  //    createTests(scenarios, CostMorselTestConfig)
  //  }

  //  Morsel engine is not complete and executes tests very slowly
  //eg. MorselExecutionContext.createClone is not implemented
  @Ignore
  def generateBlacklistTCKTestCostMorsel(): Unit = {
    printComputedBlacklist(scenarios, CostMorselTestConfig)
    fail("Do not forget to add @ignore to this method")
  }
}
