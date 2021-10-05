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
import cypher.features.ScenarioTestHelper.createTests
import cypher.features.ScenarioTestHelper.printComputedDenylist
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.util

class InterpretedAcceptanceTests extends BaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the BaseAcceptanceTest
  private val cypherTransactionsSemanticFeature = SemanticFeature.CallSubqueryInTransactions.productPrefix
  private val cypherTransactionsWithReturnSemanticFeature = SemanticFeature.CallReturningSubqueryInTransactions.productPrefix

  // TODO: Remove once CallSubqueryInTransactions is enabled by default
  def featureSpecificSettings(featureName: String): collection.Map[Setting[_], AnyRef] = featureName match {
    case "CypherTransactionsAcceptance" => collection.Map(GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features -> java.util.Set.of(Array(
      cypherTransactionsSemanticFeature,
      cypherTransactionsWithReturnSemanticFeature,
    ):_*))
    case _ => Map.empty
  }

  @TestFactory
  def runCostInterpreted(): util.Collection[DynamicTest] = {
    createTests(scenarios, InterpretedTestConfig, () => new TestDatabaseManagementServiceBuilder(), featureName => defaultTestConfig(featureName) ++ featureSpecificSettings(featureName))
  }

  @Disabled
  def generateDenylistCostInterpreted(): Unit = {
    printComputedDenylist(scenarios, InterpretedTestConfig, () => new TestDatabaseManagementServiceBuilder())
    fail("Do not forget to add @Disabled to this method")
  }
}
