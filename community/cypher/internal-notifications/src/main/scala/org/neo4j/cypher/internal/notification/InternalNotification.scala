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
package org.neo4j.cypher.internal.notification

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType

import java.lang

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Describes a notification
 */
sealed trait InternalNotification {
  def notificationName: String = this.getClass.getSimpleName.stripSuffix("$")
}

object InternalNotifications {

  // sorted alphabetically for easier maintenance
  def allNotifications: Set[String] = Set(
    "AggregationSkippedNull",
    "AssignPrivilegeCommandHasNoEffectNotification",
    "AuthProviderNotDefined",
    "CartesianProductNotification",
    "CodeGenerationFailedNotification",
    "CordonedServersExistedDuringAllocation",
    "DeprecatedAmbiguousReferenceInSubclauseExpression",
    "DeprecatedBooleanCoercion",
    "DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression",
    "DeprecatedComplexGroupingExpressionInSubclauseExpression",
    "DeprecatedConnectComponentsPlannerPreParserOption",
    "DeprecatedEagerAnalyzerPreParserOption",
    "DeprecatedExistingDataOption",
    "DeprecatedFunctionFieldNotification",
    "DeprecatedFunctionNamespaceUsed",
    "DeprecatedFunctionNotification",
    "DeprecatedGraphReferenceNotification",
    "DeprecatedIdentifierUnicode",
    "DeprecatedIdentifierWhitespaceUnicode",
    "DeprecatedImportingWithInSubqueryCall",
    "DeprecatedIndexProviderOption",
    "DeprecatedKeywordVariableInWhenOperand",
    "DeprecatedNodesOrRelationshipsInSetClauseNotification",
    "DeprecatedOptionInOptionMap",
    "DeprecatedPrecedenceOfLabelExpressionPredicate",
    "DeprecatedProcedureFieldNotification",
    "DeprecatedProcedureNamespaceUsed",
    "DeprecatedProcedureNotification",
    "DeprecatedProcedureReturnFieldNotification",
    "DeprecatedPropertyReferenceInCreate",
    "DeprecatedPropertyReferenceInMerge",
    "DeprecatedRelTypeSeparatorNotification",
    "DeprecatedRuntimeNotification",
    "DeprecatedSeedingOption",
    "DeprecatedStoreFormat",
    "DeprecatedTextIndexProvider",
    "DeprecatedWhereVariableInNodePattern",
    "DeprecatedWhereVariableInRelationshipPattern",
    "EagerLoadCsvNotification",
    "ExhaustiveShortestPathForbiddenNotification",
    "ExternalAuthNotEnabled",
    "FixedLengthRelationshipInShortestPath",
    "DeprecatedGraphReferenceNotification",
    "DeprecatedRuntimeNotification",
    "DeprecatedTextIndexProvider",
    "DeprecatedIdentifierWhitespaceUnicode",
    "DeprecatedIdentifierUnicode",
    "UnsatisfiableRelationshipTypeExpression",
    "RuntimeUnsatisfiableRelationshipTypeExpression",
    "RepeatedRelationshipReference",
    "RepeatedVarLengthRelationshipReference",
    "DeprecatedConnectComponentsPlannerPreParserOption",
    "DeprecatedEagerAnalyzerPreParserOption",
    "RetiredPlannerVersionPreParserOption",
    "AssignPrivilegeCommandHasNoEffectNotification",
    "RevokePrivilegeCommandHasNoEffectNotification",
    "GrantRoleCommandHasNoEffectNotification",
    "GrantRoleToAuthRuleCommandHasNoEffectNotification",
    "HomeDatabaseNotPresent",
    "IdentifierShadowsVariableNotification",
    "ImpossibleRevokeCommandWarning",
    "IndexHintUnfulfillableNotification",
    "IndexOrConstraintAlreadyExistsNotification",
    "IndexOrConstraintDoesNotExistNotification",
    "InsecureProtocol",
    "JoinHintUnfulfillableNotification",
    "LargeLabelWithLoadCsvNotification",
    "LocalFunctionShadowsNonLocal",
    "LocalProcedureShadowsNonLocal",
    "MissingLabelNotification",
    "MissingParametersNotification",
    "MissingPropertyNameNotification",
    "MissingRelTypeNotification",
    "NoDatabasesReallocated",
    "NodeIndexLookupUnfulfillableNotification",
    "OidcCredentialForwardingNotEnabled",
    "ProcedureWarningNotification",
    "RedundantOptionalProcedure",
    "RedundantOptionalSubquery",
    "RelationshipIndexLookupUnfulfillableNotification",
    "RepeatedRelationshipReference",
    "RepeatedVarLengthRelationshipReference",
    "RequestedTopologyMatchedCurrentTopology",
    "RevokePrivilegeCommandHasNoEffectNotification",
    "RevokeRoleCommandHasNoEffectNotification",
    "RevokeRoleFromAuthRuleCommandHasNoEffectNotification",
    "RuntimeUnsatisfiableRelationshipTypeExpression",
    "RuntimeUnsupportedNotification",
    "ServerAlreadyCordoned",
    "ServerAlreadyEnabled",
    "ShadowingInternalFunction",
    "ShardedPerformanceNotification",
    "SubqueryVariableShadowing",
    "UnboundedShortestPathNotification",
    "UnsatisfiableRelationshipTypeExpression",
    "VectorIndexDimensionsNotSpecifiedNotification",
    "VirtualGraphPostProcessingNotification",
    "WaitServerCatchingUp",
    "WaitServerCaughtUp",
    "WaitServerFailed",
    "WaitServerUnavailable"
  )

  def allNotificationsAsJavaIterable(): lang.Iterable[String] = allNotifications.asJava
}

case class CartesianProductNotification(position: InputPosition, isolatedVariables: Set[String], pattern: String)
    extends InternalNotification

case class UnboundedShortestPathNotification(position: InputPosition, pattern: String) extends InternalNotification

case class DeprecatedFunctionNotification(position: InputPosition, oldName: String, newName: Option[String])
    extends InternalNotification

case class DeprecatedRelTypeSeparatorNotification(
  position: InputPosition,
  oldExpression: String,
  rewrittenExpression: String
) extends InternalNotification

case class DeprecatedNodesOrRelationshipsInSetClauseNotification(
  position: InputPosition,
  deprecated: String,
  replacement: String
) extends InternalNotification

case class DeprecatedPropertyReferenceInCreate(position: InputPosition, varName: String) extends InternalNotification

case class DeprecatedPropertyReferenceInMerge(position: InputPosition, varName: String) extends InternalNotification

case class SubqueryVariableShadowing(position: InputPosition, subqueryType: String, varName: String)
    extends InternalNotification

case class RedundantOptionalProcedure(position: InputPosition, proc: String) extends InternalNotification

case class RedundantOptionalSubquery(position: InputPosition) extends InternalNotification

case class DeprecatedComplexGroupingExpressionInSubclauseExpression(
  position: InputPosition,
  subclauseExpression: String,
  groupingExpression: String
) extends InternalNotification

case class DeprecatedAmbiguousReferenceInSubclauseExpression(
  position: InputPosition,
  subclauseExpression: String,
  variables: String
) extends InternalNotification

case class DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression(
  position: InputPosition,
  subclauseExpression: String,
  variables: String,
  groupingExpression: String
) extends InternalNotification

case class DeprecatedImportingWithInSubqueryCall(position: InputPosition, subqueryType: String, variable: String)
    extends InternalNotification

case class DeprecatedWhereVariableInNodePattern(position: InputPosition, variableName: String, properties: String)
    extends InternalNotification

case class DeprecatedWhereVariableInRelationshipPattern(
  position: InputPosition,
  variableName: String,
  properties: String
) extends InternalNotification

case class DeprecatedPrecedenceOfLabelExpressionPredicate(position: InputPosition, labelExpression: String)
    extends InternalNotification

case class DeprecatedKeywordVariableInWhenOperand(
  position: InputPosition,
  variableName: String,
  remainingExpression: String
) extends InternalNotification

case class HomeDatabaseNotPresent(databaseName: String) extends InternalNotification

case class FixedLengthRelationshipInShortestPath(position: InputPosition, deprecated: String, replacement: String)
    extends InternalNotification

case class DeprecatedGraphReferenceNotification(deprecatedName: String, futureName: String, position: InputPosition)
    extends InternalNotification

case class DeprecatedRuntimeNotification(msg: String, oldOption: String, newOption: String)
    extends InternalNotification

case class DeprecatedTextIndexProvider(position: InputPosition) extends InternalNotification
case class DeprecatedIndexProviderOption() extends InternalNotification

case class DeprecatedIdentifierWhitespaceUnicode(position: InputPosition, unicode: Char, identifier: String)
    extends InternalNotification

case class DeprecatedIdentifierUnicode(position: InputPosition, unicode: Char, identifier: String)
    extends InternalNotification

case class UnsatisfiableRelationshipTypeExpression(position: InputPosition, labelExpression: String)
    extends InternalNotification

case class RepeatedRelationshipReference(position: InputPosition, relName: String, pattern: String)
    extends InternalNotification

case class RepeatedVarLengthRelationshipReference(position: InputPosition, relName: String, pattern: String)
    extends InternalNotification

case class DeprecatedConnectComponentsPlannerPreParserOption(position: InputPosition) extends InternalNotification
case class DeprecatedEagerAnalyzerPreParserOption(position: InputPosition) extends InternalNotification
case class RetiredPlannerVersionPreParserOption(position: InputPosition, version: String) extends InternalNotification

case class AuthProviderNotDefined(provider: String) extends InternalNotification
case class ExternalAuthNotEnabled() extends InternalNotification

case class OidcCredentialForwardingNotEnabled() extends InternalNotification

case class AssignPrivilegeCommandHasNoEffectNotification(command: String) extends InternalNotification
case class RevokePrivilegeCommandHasNoEffectNotification(command: String) extends InternalNotification
case class GrantRoleCommandHasNoEffectNotification(command: String) extends InternalNotification
case class RevokeRoleCommandHasNoEffectNotification(command: String) extends InternalNotification
case class GrantRoleToAuthRuleCommandHasNoEffectNotification(command: String) extends InternalNotification
case class RevokeRoleFromAuthRuleCommandHasNoEffectNotification(command: String) extends InternalNotification

case class ShardedPerformanceNotification() extends InternalNotification

case object VirtualGraphPostProcessingNotification extends InternalNotification

case class ImpossibleRevokeCommandWarning(command: String, cause: String) extends InternalNotification

case class ServerAlreadyEnabled(server: String) extends InternalNotification
case class ServerAlreadyCordoned(server: String) extends InternalNotification
case class NoDatabasesReallocated() extends InternalNotification
case class CordonedServersExistedDuringAllocation(servers: Seq[String]) extends InternalNotification
case class RequestedTopologyMatchedCurrentTopology() extends InternalNotification

case class IndexOrConstraintAlreadyExistsNotification(command: String, conflicting: String)
    extends InternalNotification
case class IndexOrConstraintDoesNotExistNotification(command: String, name: String) extends InternalNotification
case object VectorIndexDimensionsNotSpecifiedNotification extends InternalNotification
case object AggregationSkippedNull extends InternalNotification

case object DeprecatedBooleanCoercion extends InternalNotification {
  def instance: DeprecatedBooleanCoercion.type = this
}

case class DeprecatedOptionInOptionMap(oldOption: String, replacmentOption: String) extends InternalNotification
case class DeprecatedSeedingOption(oldOption: String) extends InternalNotification
case class DeprecatedExistingDataOption() extends InternalNotification
case class DeprecatedStoreFormat(format: String) extends InternalNotification

case object InsecureProtocol extends InternalNotification

case class RuntimeUnsatisfiableRelationshipTypeExpression(types: List[String]) extends InternalNotification {

  def this(types: java.util.List[String]) =
    this(types.asScala.toList)
}

case class WaitServerUnavailable(serverName: String) extends InternalNotification {}
case class WaitServerCatchingUp(serverName: String, boltAddress: String) extends InternalNotification {}
case class WaitServerFailed(serverName: String, boltAddress: String, message: String) extends InternalNotification {}
case class WaitServerCaughtUp(serverName: String, boltAddress: String) extends InternalNotification {}

case class RuntimeUnsupportedNotification(
  failingRuntimeConf: String,
  fallbackRuntimeConf: String,
  msg: String
) extends InternalNotification

case class IndexHintUnfulfillableNotification(
  variableName: String,
  labelOrRelType: String,
  propertyKeys: Seq[String],
  entityType: EntityType,
  indexType: IndexHintIndexType
) extends InternalNotification

case class JoinHintUnfulfillableNotification(identified: Seq[String]) extends InternalNotification

case class NodeIndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case class RelationshipIndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case object EagerLoadCsvNotification extends InternalNotification

case class LargeLabelWithLoadCsvNotification(labelName: String) extends InternalNotification

case class MissingLabelNotification(position: InputPosition, label: String, db: String) extends InternalNotification

case class MissingRelTypeNotification(position: InputPosition, relType: String, db: String) extends InternalNotification

case class MissingPropertyNameNotification(position: InputPosition, name: String, db: String)
    extends InternalNotification

case class ExhaustiveShortestPathForbiddenNotification(position: InputPosition, pathPredicates: Set[String])
    extends InternalNotification

case class DeprecatedProcedureNotification(position: InputPosition, oldName: String, newName: Option[String])
    extends InternalNotification

case class ProcedureWarningNotification(position: InputPosition, procedure: String, warning: String)
    extends InternalNotification

case class DeprecatedProcedureReturnFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class DeprecatedProcedureFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class DeprecatedFunctionFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class DeprecatedFunctionNamespaceUsed(position: InputPosition, callable: String)
    extends InternalNotification

case class DeprecatedProcedureNamespaceUsed(position: InputPosition, callable: String)
    extends InternalNotification

case class ShadowingInternalFunction(position: InputPosition, callable: String)
    extends InternalNotification

case class MissingParametersNotification(parameters: Seq[String]) extends InternalNotification

case class CodeGenerationFailedNotification(
  failingRuntimeConf: String,
  fallbackRuntimeConf: String,
  msg: String
) extends InternalNotification

case class IdentifierShadowsVariableNotification(position: InputPosition, identifier: String, clause: String)
    extends InternalNotification

case class LocalProcedureShadowsNonLocal(position: InputPosition, name: String)
    extends InternalNotification

case class LocalFunctionShadowsNonLocal(position: InputPosition, name: String)
    extends InternalNotification
