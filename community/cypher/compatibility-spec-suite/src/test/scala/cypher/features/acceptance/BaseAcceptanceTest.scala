/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package cypher.features.acceptance

import cypher.features.BaseFeatureTest
import cypher.features.BaseFeatureTestHolder
import cypher.features.Neo4jAdapter.defaultTestConfig
import cypher.features.TestDatabaseProvider
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.opencypher.tools.tck.api.Scenario

abstract class BaseAcceptanceTest extends BaseFeatureTest {

  // these should be empty on commit!
  // Use a substring match, for example "UnwindAcceptance"
  val categoryToRun = ""
  val featureToRun = ""
  val scenarioToRun = ""

  override lazy val scenarios: Seq[Scenario] =
    filterScenarios(BaseFeatureTestHolder.allAcceptanceScenarios, categoryToRun, featureToRun, scenarioToRun)

  private val provider: TestDatabaseProvider =
    new TestDatabaseProvider(() => new TestDatabaseManagementServiceBuilder(), _ => {})

  override def dbProvider(): TestDatabaseProvider = {
    provider
  }

  override def dbConfigPerFeature(featureName: String): collection.Map[Setting[_], AnyRef] =
    defaultTestConfig(featureName)

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!categoryToRun.equals(""))
      fail("categoryToRun is only for debugging and should not be committed")

    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }
}
