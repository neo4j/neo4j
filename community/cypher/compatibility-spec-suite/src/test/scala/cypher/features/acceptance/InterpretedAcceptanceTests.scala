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
package cypher.features.acceptance

import cypher.features.TestConfig
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.graphdb.config.Setting

import scala.jdk.CollectionConverters.SetHasAsJava

class InterpretedAcceptanceTests extends BaseAcceptanceTest {

  // If you want to only run a specific feature or scenario, go to the BaseAcceptanceTest

  override val config: TestConfig = TestConfig.interpreted(getClass)

  override def dbConfigPerFeature(featureName: String): collection.Map[Setting[_], AnyRef] =
    super.dbConfigPerFeature(featureName) ++
      featureDependentSettings(featureName)

  override val useBolt: Boolean = false

  def featureDependentSettings(featureName: String): collection.Map[Setting[_], Object] = featureName match {
    case "QuantifiedPathPatternAcceptance" =>
      enableSemanticFeatures(Set(
        SemanticFeature.QuantifiedPathPatterns
      ))
    case _ => Map.empty
  }

  private def enableSemanticFeatures(features: Set[SemanticFeature]): Map[Setting[_], Object] = Map[Setting[_], Object](
    GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features ->
      features.map(_.productPrefix).asJava
  )
}
