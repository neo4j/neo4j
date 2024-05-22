/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics

sealed trait SemanticFeature extends Product

sealed trait FeatureToString {
  override def toString: String = name
  def name: String
}

object SemanticFeature {

  case object MultipleDatabases extends SemanticFeature with FeatureToString {
    override def name: String = "multiple databases"
  }

  case object ShowSetting extends SemanticFeature with FeatureToString {
    override def name: String = "show setting"
  }

  case object MultipleGraphs extends SemanticFeature with FeatureToString {
    override def name: String = "multiple graphs"
  }

  /**
   * USE clauses are allowed and USE clauses in a query can target different graphs.
   * This feature also means that dynamic USE clauses are allowed.
   */
  case object UseAsMultipleGraphsSelector extends SemanticFeature with FeatureToString {
    override def name: String = "USE multiple graph selector"
  }

  /**
   * USE clauses are allowed, but all USE clauses in a query must target the same graph.
   * This feature also does not allow dynamic USE clauses.
   */
  case object UseAsSingleGraphSelector extends SemanticFeature with FeatureToString {
    override def name: String = "USE single graph selector"
  }

  case object GpmShortestPath extends SemanticFeature with FeatureToString {
    override def name: String = "Shortest path as defined for GQL"
  }

  case object MatchModes extends SemanticFeature with FeatureToString {
    override def name: String = "Match modes"
  }

  case object ComposableCommands extends SemanticFeature with FeatureToString {
    override def name: String = "composable commands"
  }

  private val allSemanticFeatures = Set(
    MultipleDatabases,
    MultipleGraphs,
    UseAsMultipleGraphsSelector,
    UseAsSingleGraphSelector,
    ShowSetting,
    GpmShortestPath,
    MatchModes,
    ComposableCommands
  )

  def fromString(str: String): SemanticFeature =
    allSemanticFeatures.find(_.productPrefix == str).getOrElse(
      throw new IllegalArgumentException(
        s"No such SemanticFeature: $str. Valid options are: ${allSemanticFeatures.map(_.productPrefix).mkString(", ")}"
      )
    )

  def checkFeatureCompatibility(features: Set[SemanticFeature]): Unit =
    if (
      features.contains(SemanticFeature.UseAsSingleGraphSelector)
      && features.contains(SemanticFeature.UseAsMultipleGraphsSelector)
    )
      throw new IllegalArgumentException(s"Semantic features ${SemanticFeature.UseAsSingleGraphSelector} " +
        s"and ${SemanticFeature.UseAsMultipleGraphsSelector} are incompatible and cannot be enabled at the same time")

}
