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

  case object OidcCredentialForwarding extends SemanticFeature with FeatureToString {
    override def name: String = "OIDC credential forwarding"
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

  case object ComposableCommands extends SemanticFeature with FeatureToString {
    override def name: String = "composable commands"
  }

  case object GraphTypes extends SemanticFeature with FeatureToString {
    override def name: String = "`GRAPH TYPE` schema management"
  }

  case object ExperimentalCypherVersions extends SemanticFeature with FeatureToString {
    override def name: String = "experimental cypher versions"
  }

  case object RelationshipPropertyValueAccessRules extends SemanticFeature with FeatureToString {
    override def name: String = "Property value access rules on relationships"
  }

  case object VectorSearchWithComplexPattern extends SemanticFeature with FeatureToString {
    override def name: String = "vector search with complex pattern"
  }

  case object UUIDType extends SemanticFeature with FeatureToString {
    override def name: String = "UUID type"
  }

  case object GroupByClause extends SemanticFeature with FeatureToString {
    override def name: String = "Group By clause"
  }

  case object LocalCallables extends SemanticFeature with FeatureToString {
    override def name: String = "local callables"
  }

  case object ScopeQueries extends SemanticFeature with FeatureToString {
    override def name: String = "scope queries"
  }

  case object EnableWorkingScopeNamespacer extends SemanticFeature with FeatureToString {
    override def name: String = "enable working scope implementation for namespacer"
  }

  case object EnableParsingOfObfuscatedLiterals extends SemanticFeature with FeatureToString {
    override def name: String = "enable parsing of obfuscated literals"
  }

  case object DisableTypeCheckingInSemanticAnalysis extends SemanticFeature with FeatureToString {
    override def name: String = "disable type checking in semantic analysis"
  }

  /**
   * Normally, it's not allowed to mix old and new label expression syntax within a clause.
   * The implementation of this check has had bugs in the past, can be inconvenient for users
   * and is not necessary for any functionality.
   * This semantic feature exists to have a workaround of future bugs in semantic analysis.
   * Note, even with this feature enabled, it's still not allowed to mix syntax within a single expression.
   */
  case object AllowClauseWithMixedLabelSyntax extends SemanticFeature with FeatureToString {
    override def name: String = "Allow mixing old and new label expression syntax in clauses"
  }

  case object AttributeBasedAccessControl extends SemanticFeature with FeatureToString {
    override def name: String = "Attribute based access control"
  }

  /**
   * Allows `USING EXPAND ...` to force expand direction and order
   */
  case object ExpandHints extends SemanticFeature with FeatureToString {
    override def name: String = "Expand hints"
  }

  case object UserTags extends SemanticFeature with FeatureToString {
    override def name: String = "User tags"
  }

  case object ValueInListProperty extends SemanticFeature with FeatureToString {
    override def name: String = "access rules checking for a value in a list property"
  }

  case object SecretsManager extends SemanticFeature with FeatureToString {
    override def name: String = "secrets manager"
  }

  private val allSemanticFeatures = Set(
    MultipleDatabases,
    MultipleGraphs,
    UseAsMultipleGraphsSelector,
    UseAsSingleGraphSelector,
    ShowSetting,
    OidcCredentialForwarding,
    ComposableCommands,
    GraphTypes,
    ExperimentalCypherVersions,
    RelationshipPropertyValueAccessRules,
    VectorSearchWithComplexPattern,
    UUIDType,
    LocalCallables,
    ScopeQueries,
    EnableWorkingScopeNamespacer,
    EnableParsingOfObfuscatedLiterals,
    DisableTypeCheckingInSemanticAnalysis,
    AllowClauseWithMixedLabelSyntax,
    AttributeBasedAccessControl,
    ExpandHints,
    UserTags,
    GroupByClause,
    ValueInListProperty,
    SecretsManager
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
