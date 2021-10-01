/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import cypher.features.Neo4jAdapter.defaultTestConfig
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.opencypher.tools.tck.api.Scenario

abstract class BaseTCKTests extends BaseFeatureTest {

  // these two should be empty on commit!
  val categoryToRun = ""
  val featureToRun = ""
  val scenarioToRun = ""

  lazy val scenarios: Seq[Scenario] = filterScenarios(BaseFeatureTestHolder.allTckScenarios, categoryToRun, featureToRun, scenarioToRun)

  override def graphDatabaseFactory(): TestDatabaseManagementServiceBuilder = new TestDatabaseManagementServiceBuilder()

  override def dbConfigPerFeature(featureName: String): collection.Map[Setting[_], AnyRef] = defaultTestConfig(featureName)

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
