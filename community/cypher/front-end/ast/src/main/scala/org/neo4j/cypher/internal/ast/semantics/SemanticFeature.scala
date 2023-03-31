/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

  case object UseGraphSelector extends SemanticFeature with FeatureToString {
    override def name: String = "USE graph selector"
  }

  case object ExpressionsInViewInvocations extends SemanticFeature

  case object QuantifiedPathPatterns extends SemanticFeature with FeatureToString {
    override def name: String = "Quantified path patterns"
  }

  case object GpmShortestPath extends SemanticFeature with FeatureToString {
    override def name: String = "Shortest path as defined for GQL"
  }

  private val allSemanticFeatures = Set(
    MultipleDatabases,
    MultipleGraphs,
    UseGraphSelector,
    ExpressionsInViewInvocations,
    QuantifiedPathPatterns,
    ShowSetting,
    GpmShortestPath
  )

  def fromString(str: String): SemanticFeature =
    allSemanticFeatures.find(_.productPrefix == str).getOrElse(
      throw new IllegalArgumentException(
        s"No such SemanticFeature: $str. Valid options are: ${allSemanticFeatures.map(_.productPrefix).mkString(", ")}"
      )
    )
}
