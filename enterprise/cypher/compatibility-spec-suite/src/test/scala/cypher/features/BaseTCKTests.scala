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

import org.junit.Assert.fail
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.opencypher.tools.tck.api.{CypherTCK, Scenario}

@RunWith(classOf[JUnitPlatform])
abstract class BaseTCKTests extends BaseFeatureTest {

  // these two should be empty on commit!
  val featureToRun = ""
  val scenarioToRun = ""

  val scenarios: Seq[Scenario] = filterScenarios(CypherTCK.allTckScenarios, featureToRun, scenarioToRun)

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }
}
