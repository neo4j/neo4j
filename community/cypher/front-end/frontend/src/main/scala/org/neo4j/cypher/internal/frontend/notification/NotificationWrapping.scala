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
package org.neo4j.cypher.internal.frontend.notification

import org.neo4j.cypher.internal.notification.AggregationSkippedNull
import org.neo4j.cypher.internal.notification.AssignPrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.AuthProviderNotDefined
import org.neo4j.cypher.internal.notification.CartesianProductNotification
import org.neo4j.cypher.internal.notification.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.notification.CordonedServersExistedDuringAllocation
import org.neo4j.cypher.internal.notification.DeprecatedAmbiguousReferenceInSubclauseExpression
import org.neo4j.cypher.internal.notification.DeprecatedBooleanCoercion
import org.neo4j.cypher.internal.notification.DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression
import org.neo4j.cypher.internal.notification.DeprecatedComplexGroupingExpressionInSubclauseExpression
import org.neo4j.cypher.internal.notification.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.notification.DeprecatedEagerAnalyzerPreParserOption
import org.neo4j.cypher.internal.notification.DeprecatedExistingDataOption
import org.neo4j.cypher.internal.notification.DeprecatedFunctionFieldNotification
import org.neo4j.cypher.internal.notification.DeprecatedFunctionNamespaceUsed
import org.neo4j.cypher.internal.notification.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.notification.DeprecatedGraphReferenceNotification
import org.neo4j.cypher.internal.notification.DeprecatedIdentifierUnicode
import org.neo4j.cypher.internal.notification.DeprecatedIdentifierWhitespaceUnicode
import org.neo4j.cypher.internal.notification.DeprecatedImportingWithInSubqueryCall
import org.neo4j.cypher.internal.notification.DeprecatedIndexProviderOption
import org.neo4j.cypher.internal.notification.DeprecatedKeywordVariableInWhenOperand
import org.neo4j.cypher.internal.notification.DeprecatedNodesOrRelationshipsInSetClauseNotification
import org.neo4j.cypher.internal.notification.DeprecatedOptionInOptionMap
import org.neo4j.cypher.internal.notification.DeprecatedPrecedenceOfLabelExpressionPredicate
import org.neo4j.cypher.internal.notification.DeprecatedProcedureFieldNotification
import org.neo4j.cypher.internal.notification.DeprecatedProcedureNamespaceUsed
import org.neo4j.cypher.internal.notification.DeprecatedProcedureNotification
import org.neo4j.cypher.internal.notification.DeprecatedProcedureReturnFieldNotification
import org.neo4j.cypher.internal.notification.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.notification.DeprecatedPropertyReferenceInMerge
import org.neo4j.cypher.internal.notification.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.notification.DeprecatedRuntimeNotification
import org.neo4j.cypher.internal.notification.DeprecatedSeedingOption
import org.neo4j.cypher.internal.notification.DeprecatedStoreFormat
import org.neo4j.cypher.internal.notification.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.notification.DeprecatedWhereVariableInNodePattern
import org.neo4j.cypher.internal.notification.DeprecatedWhereVariableInRelationshipPattern
import org.neo4j.cypher.internal.notification.EagerLoadCsvNotification
import org.neo4j.cypher.internal.notification.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.notification.ExternalAuthNotEnabled
import org.neo4j.cypher.internal.notification.FixedLengthRelationshipInShortestPath
import org.neo4j.cypher.internal.notification.GrantRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.GrantRoleToAuthRuleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.notification.IdentifierShadowsVariableNotification
import org.neo4j.cypher.internal.notification.ImpossibleRevokeCommandWarning
import org.neo4j.cypher.internal.notification.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.notification.IndexOrConstraintAlreadyExistsNotification
import org.neo4j.cypher.internal.notification.IndexOrConstraintDoesNotExistNotification
import org.neo4j.cypher.internal.notification.InsecureProtocol
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.JoinHintUnfulfillableNotification
import org.neo4j.cypher.internal.notification.LargeLabelWithLoadCsvNotification
import org.neo4j.cypher.internal.notification.LocalFunctionShadowsNonLocal
import org.neo4j.cypher.internal.notification.LocalProcedureShadowsNonLocal
import org.neo4j.cypher.internal.notification.MissingLabelNotification
import org.neo4j.cypher.internal.notification.MissingParametersNotification
import org.neo4j.cypher.internal.notification.MissingPropertyNameNotification
import org.neo4j.cypher.internal.notification.MissingRelTypeNotification
import org.neo4j.cypher.internal.notification.NoDatabasesReallocated
import org.neo4j.cypher.internal.notification.NodeIndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.notification.OidcCredentialForwardingNotEnabled
import org.neo4j.cypher.internal.notification.ProcedureWarningNotification
import org.neo4j.cypher.internal.notification.RedundantOptionalProcedure
import org.neo4j.cypher.internal.notification.RedundantOptionalSubquery
import org.neo4j.cypher.internal.notification.RelationshipIndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.notification.RepeatedRelationshipReference
import org.neo4j.cypher.internal.notification.RepeatedVarLengthRelationshipReference
import org.neo4j.cypher.internal.notification.RequestedTopologyMatchedCurrentTopology
import org.neo4j.cypher.internal.notification.RetiredPlannerVersionPreParserOption
import org.neo4j.cypher.internal.notification.RevokePrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.RevokeRoleFromAuthRuleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.RuntimeUnsatisfiableRelationshipTypeExpression
import org.neo4j.cypher.internal.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.notification.ServerAlreadyCordoned
import org.neo4j.cypher.internal.notification.ServerAlreadyEnabled
import org.neo4j.cypher.internal.notification.ShadowingInternalFunction
import org.neo4j.cypher.internal.notification.ShardedPerformanceNotification
import org.neo4j.cypher.internal.notification.SubqueryVariableShadowing
import org.neo4j.cypher.internal.notification.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.notification.UnsatisfiableRelationshipTypeExpression
import org.neo4j.cypher.internal.notification.VectorIndexDimensionsNotSpecifiedNotification
import org.neo4j.cypher.internal.notification.VirtualGraphPostProcessingNotification
import org.neo4j.cypher.internal.notification.WaitServerCatchingUp
import org.neo4j.cypher.internal.notification.WaitServerCaughtUp
import org.neo4j.cypher.internal.notification.WaitServerFailed
import org.neo4j.cypher.internal.notification.WaitServerUnavailable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb
import org.neo4j.notifications.NotificationCodeWithDescription
import org.neo4j.notifications.NotificationDetail
import org.neo4j.notifications.NotificationImplementation

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

object NotificationWrapping {

  def asKernelNotificationJava(
    offset: Option[InputPosition],
    notification: InternalNotification
  ): NotificationImplementation = {
    asKernelNotification(offset)(notification)
  }

  def asKernelNotification(offset: Option[InputPosition])(notification: InternalNotification)
    : NotificationImplementation = notification match {
    case CartesianProductNotification(pos, variables, pattern) =>
      NotificationCodeWithDescription.cartesianProduct(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.cartesianProductDescription(variables.asJava),
        pattern
      )
    case RuntimeUnsupportedNotification(failingConf, fallbackRuntimeConf, cause) =>
      NotificationCodeWithDescription.runtimeUnsupported(
        graphdb.InputPosition.empty,
        failingConf,
        fallbackRuntimeConf,
        cause
      )
    case IndexHintUnfulfillableNotification(variableName, label, propertyKeys, entityType, indexType) =>
      NotificationCodeWithDescription.indexHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.indexHint(entityType, indexType, variableName, label, propertyKeys: _*),
        NotificationDetail.index(indexType, label, propertyKeys.asJava)
      )
    case JoinHintUnfulfillableNotification(variables) =>
      val javaVariables = variables.asJava
      NotificationCodeWithDescription.joinHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.joinKey(javaVariables),
        javaVariables
      )
    case NodeIndexLookupUnfulfillableNotification(labels) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.nodeIndexSeekOrScan(labels.asJava),
        labels.toSeq.asJava
      )
    case RelationshipIndexLookupUnfulfillableNotification(relTypes) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.relationshipIndexSeekOrScan(relTypes.asJava),
        relTypes.toSeq.asJava
      )
    case EagerLoadCsvNotification =>
      NotificationCodeWithDescription.eagerLoadCsv(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification(labelName) =>
      NotificationCodeWithDescription.largeLabelLoadCsv(
        graphdb.InputPosition.empty,
        labelName
      )
    case MissingLabelNotification(pos, label, db) =>
      NotificationCodeWithDescription.missingLabel(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingLabel(label),
        label,
        db
      )
    case MissingRelTypeNotification(pos, relType, db) =>
      NotificationCodeWithDescription.missingRelType(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingRelationshipType(relType),
        relType,
        db
      )
    case MissingPropertyNameNotification(pos, name, db) =>
      NotificationCodeWithDescription.missingPropertyName(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.propertyName(name),
        name,
        db
      )
    case UnboundedShortestPathNotification(pos, pattern) =>
      NotificationCodeWithDescription.unboundedShortestPath(pos.withOffset(offset).asInputPosition, pattern)
    case ExhaustiveShortestPathForbiddenNotification(pos, pathPredicates) =>
      NotificationCodeWithDescription.exhaustiveShortestPath(
        pos.withOffset(offset).asInputPosition,
        pathPredicates.toSeq.asJava
      )
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      if (newName.isEmpty || newName.get.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedFunctionWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName.get),
          oldName,
          newName.get
        )
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      if (newName.isEmpty || newName.get.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedProcedureWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName.get),
          oldName,
          newName.get
        )
    case DeprecatedProcedureReturnFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureReturnField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedField(procedure, field),
        procedure,
        field
      )
    case DeprecatedProcedureFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedInputField(procedure, field),
        procedure,
        field
      )
    case DeprecatedFunctionFieldNotification(pos, function, field) =>
      NotificationCodeWithDescription.deprecatedFunctionField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedInputField(function, field),
        function,
        field
      )
    case DeprecatedFunctionNamespaceUsed(pos, callable) =>
      NotificationCodeWithDescription.deprecatedFunctionNamespace(pos.withOffset(offset).asInputPosition, callable)

    case DeprecatedProcedureNamespaceUsed(pos, callable) =>
      NotificationCodeWithDescription.deprecatedProcedureNamespace(pos.withOffset(offset).asInputPosition, callable)

    case ShadowingInternalFunction(pos, callable) =>
      NotificationCodeWithDescription.shadowingInternalFunction(pos.withOffset(offset).asInputPosition, callable)

    case DeprecatedRelTypeSeparatorNotification(pos, oldExpression, rewrittenExpression) =>
      NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecationNotificationDetail(rewrittenExpression),
        oldExpression,
        rewrittenExpression
      )
    case DeprecatedNodesOrRelationshipsInSetClauseNotification(pos, deprecated, replacement) =>
      NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause(
        pos.withOffset(offset).asInputPosition,
        deprecated,
        replacement
      )
    case DeprecatedPropertyReferenceInCreate(pos, name) =>
      NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate(
        pos.withOffset(offset).asInputPosition,
        name
      )
    case DeprecatedPropertyReferenceInMerge(pos, name) =>
      NotificationCodeWithDescription.deprecatedPropertyReferenceInMerge(
        pos.withOffset(offset).asInputPosition,
        name
      )
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCodeWithDescription.procedureWarning(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.procedureWarning(name, warning),
        warning,
        name
      )
    case MissingParametersNotification(parameters) =>
      NotificationCodeWithDescription.missingParameterForExplain(
        graphdb.InputPosition.empty,
        NotificationDetail.missingParameters(parameters.asJava),
        parameters.asJava
      )
    case CodeGenerationFailedNotification(failingConf, fallbackRuntimeConf, cause) =>
      NotificationCodeWithDescription.codeGenerationFailed(
        graphdb.InputPosition.empty,
        failingConf,
        fallbackRuntimeConf,
        cause
      )
    case SubqueryVariableShadowing(pos, subqueryType, varName) =>
      NotificationCodeWithDescription.subqueryVariableShadowing(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.shadowingVariable(varName),
        subqueryType,
        varName
      )
    case RedundantOptionalProcedure(pos, proc) =>
      NotificationCodeWithDescription.redundantOptionalProcedure(
        pos.withOffset(offset).asInputPosition,
        proc
      )
    case RedundantOptionalSubquery(pos) =>
      NotificationCodeWithDescription.redundantOptionalSubquery(
        pos.withOffset(offset).asInputPosition
      )
    case DeprecatedComplexGroupingExpressionInSubclauseExpression(pos, subclauseExpr, groupingExpr) =>
      NotificationCodeWithDescription.deprecatedComplexSubclauseExpression(
        pos.withOffset(offset).asInputPosition,
        subclauseExpr,
        groupingExpr
      )
    case DeprecatedAmbiguousReferenceInSubclauseExpression(pos, subclauseExpr, variablesStr) =>
      NotificationCodeWithDescription.deprecatedAmbiguousSubclauseExpression(
        pos.withOffset(offset).asInputPosition,
        subclauseExpr,
        variablesStr
      )
    case DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression(
        pos,
        subclauseExpr,
        variablesStr,
        groupingExpr
      ) =>
      NotificationCodeWithDescription.deprecatedAmbiguousAndComplexSubclauseExpression(
        pos.withOffset(offset).asInputPosition,
        subclauseExpr,
        variablesStr,
        groupingExpr
      )
    case DeprecatedImportingWithInSubqueryCall(pos, subqueryType, varName) =>
      NotificationCodeWithDescription.deprecatedImportingWithInSubqueryCall(
        pos.withOffset(offset).asInputPosition,
        subqueryType,
        varName
      )
    case DeprecatedWhereVariableInNodePattern(pos, variableName, properties) =>
      NotificationCodeWithDescription.deprecatedWhereVariableInNodePattern(
        pos.withOffset(offset).asInputPosition,
        variableName,
        properties
      )
    case DeprecatedWhereVariableInRelationshipPattern(pos, variableName, properties) =>
      NotificationCodeWithDescription.deprecatedWhereVariableInRelationshipPattern(
        pos.withOffset(offset).asInputPosition,
        variableName,
        properties
      )
    case DeprecatedPrecedenceOfLabelExpressionPredicate(pos, labelExpressionPredicate) =>
      NotificationCodeWithDescription.deprecatedPrecedenceOfLabelExpressionPredicate(
        pos.withOffset(offset).asInputPosition,
        labelExpressionPredicate
      )
    case DeprecatedKeywordVariableInWhenOperand(pos, variableName, remainingExpression) =>
      NotificationCodeWithDescription.deprecatedKeywordVariableInWhenOperand(
        pos.withOffset(offset).asInputPosition,
        variableName,
        remainingExpression
      )
    case HomeDatabaseNotPresent(name) => NotificationCodeWithDescription.homeDatabaseNotPresent(
        InputPosition.NONE.asInputPosition,
        s"HOME DATABASE: $name",
        name
      )
    case FixedLengthRelationshipInShortestPath(pos, deprecated, replacement) =>
      NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship(
        pos.withOffset(offset).asInputPosition,
        deprecated,
        replacement
      )

    case DeprecatedTextIndexProvider(pos) =>
      NotificationCodeWithDescription.deprecatedTextIndexProvider(
        pos.withOffset(offset).asInputPosition
      )

    case DeprecatedIndexProviderOption() =>
      NotificationCodeWithDescription.deprecatedIndexProviderOption(
        graphdb.InputPosition.empty
      )

    case DeprecatedGraphReferenceNotification(deprecatedName, futureName, position) =>
      NotificationCodeWithDescription.deprecatedGraphReferenceNotification(
        deprecatedName,
        futureName,
        position.withOffset(offset).asInputPosition
      )

    case DeprecatedRuntimeNotification(msg, oldOption, newOption) =>
      NotificationCodeWithDescription.deprecatedRuntimeOption(
        graphdb.InputPosition.empty,
        msg,
        oldOption,
        newOption
      )

    case UnsatisfiableRelationshipTypeExpression(position, relTypeExpression) =>
      NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.unsatisfiableRelTypeExpression(relTypeExpression),
        relTypeExpression
      )

    case RuntimeUnsatisfiableRelationshipTypeExpression(types) =>
      val stringified = types.mkString("&")
      NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression(
        InputPosition.NONE.asInputPosition,
        NotificationDetail.unsatisfiableRelTypeExpression(stringified),
        stringified
      )

    case RepeatedRelationshipReference(position, relName, pattern) =>
      NotificationCodeWithDescription.repeatedRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedRelationship(relName),
        relName,
        pattern
      )

    case RepeatedVarLengthRelationshipReference(position, relName, pattern) =>
      NotificationCodeWithDescription.repeatedVarLengthRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedVarLengthRel(relName),
        relName,
        pattern
      )

    case IdentifierShadowsVariableNotification(position, identifier, clause) =>
      NotificationCodeWithDescription.identifierShadowingVariable(
        position.withOffset(offset).asInputPosition,
        identifier,
        clause
      )

    case LocalProcedureShadowsNonLocal(position, name) =>
      NotificationCodeWithDescription.callableShadowing(
        position.withOffset(offset).asInputPosition,
        "procedure",
        name
      )

    case LocalFunctionShadowsNonLocal(position, name) =>
      NotificationCodeWithDescription.callableShadowing(
        position.withOffset(offset).asInputPosition,
        "function",
        name
      )

    case DeprecatedIdentifierWhitespaceUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode(
        position.withOffset(offset).asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedIdentifierUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierUnicode(
        position.withOffset(offset).asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedConnectComponentsPlannerPreParserOption(position) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption(
        position.asInputPosition
      )

    case DeprecatedEagerAnalyzerPreParserOption(position) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription
        .deprecatedEagerAnalyzerPreParserOption(position.asInputPosition)

    case RetiredPlannerVersionPreParserOption(position, version) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription
        .retiredPlannerVersionPreParserOption(position.asInputPosition, version)

    case DeprecatedOptionInOptionMap(oldOption, newOption) =>
      NotificationCodeWithDescription.deprecatedOptionInOptionMap(oldOption, newOption)

    case DeprecatedSeedingOption(option) => NotificationCodeWithDescription.deprecatedSeedingOption(option)

    case DeprecatedExistingDataOption() => NotificationCodeWithDescription.deprecatedExistingDataOption()

    case DeprecatedStoreFormat(format) =>
      NotificationCodeWithDescription.deprecatedStoreFormat(format)

    case AuthProviderNotDefined(provider) =>
      NotificationCodeWithDescription.authProviderNotDefined(
        graphdb.InputPosition.empty,
        provider
      )

    case _: ExternalAuthNotEnabled =>
      NotificationCodeWithDescription.externalAuthNotEnabled(graphdb.InputPosition.empty)

    case _: OidcCredentialForwardingNotEnabled =>
      NotificationCodeWithDescription.oidcCredentialForwardingNotEnabled(graphdb.InputPosition.empty)

    case AssignPrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectAssignPrivilege(
        graphdb.InputPosition.empty,
        command
      )

    case RevokePrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectRevokePrivilege(
        graphdb.InputPosition.empty,
        command
      )

    case GrantRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectGrantRole(
        graphdb.InputPosition.empty,
        command
      )

    case RevokeRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectRevokeRole(
        graphdb.InputPosition.empty,
        command
      )

    case GrantRoleToAuthRuleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectGrantRoleToAuthRule(
        graphdb.InputPosition.empty,
        command
      )

    case RevokeRoleFromAuthRuleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffectRevokeRoleToAuthRule(
        graphdb.InputPosition.empty,
        command
      )

    case ShardedPerformanceNotification() =>
      NotificationCodeWithDescription.shardedPerformance()

    case VirtualGraphPostProcessingNotification =>
      NotificationCodeWithDescription.graphEngineFallbackPostProcessing()

    case IndexOrConstraintAlreadyExistsNotification(command, conflicting) =>
      NotificationCodeWithDescription.indexOrConstraintAlreadyExists(
        graphdb.InputPosition.empty,
        command,
        conflicting
      )

    case IndexOrConstraintDoesNotExistNotification(command, name) =>
      NotificationCodeWithDescription.indexOrConstraintDoesNotExist(
        graphdb.InputPosition.empty,
        command,
        name
      )

    case VectorIndexDimensionsNotSpecifiedNotification =>
      NotificationCodeWithDescription.vectorIndexDimensionsNotSpecified(graphdb.InputPosition.empty)

    case ImpossibleRevokeCommandWarning(command, cause) =>
      NotificationCodeWithDescription.impossibleRevokeCommand(
        graphdb.InputPosition.empty,
        command,
        cause
      )

    case ServerAlreadyEnabled(server) =>
      NotificationCodeWithDescription.serverAlreadyEnabled(
        graphdb.InputPosition.empty,
        server
      )

    case ServerAlreadyCordoned(server) =>
      NotificationCodeWithDescription.serverAlreadyCordoned(
        graphdb.InputPosition.empty,
        server
      )

    case NoDatabasesReallocated() =>
      NotificationCodeWithDescription.noDatabasesReallocated(
        graphdb.InputPosition.empty
      )

    case CordonedServersExistedDuringAllocation(servers) =>
      NotificationCodeWithDescription.cordonedServersExist(
        graphdb.InputPosition.empty,
        servers.asJava
      )

    case RequestedTopologyMatchedCurrentTopology() =>
      NotificationCodeWithDescription.requestedTopologyMatchedCurrentTopology(
        graphdb.InputPosition.empty
      )

    case AggregationSkippedNull    => NotificationCodeWithDescription.aggregationSkippedNull()
    case DeprecatedBooleanCoercion => NotificationCodeWithDescription.deprecatedBooleanCoercion()
    case InsecureProtocol          => NotificationCodeWithDescription.insecureProtocol()

    case WaitServerUnavailable(serverName) => NotificationCodeWithDescription.waitServerUnavailable(serverName)
    case WaitServerCatchingUp(serverName, serverAddress) =>
      NotificationCodeWithDescription.waitServerCatchingUp(serverName, serverAddress)
    case WaitServerFailed(serverName, serverAddress, error) =>
      NotificationCodeWithDescription.waitServerFailed(serverName, serverAddress, error)
    case WaitServerCaughtUp(serverName, serverAddress) =>
      NotificationCodeWithDescription.waitServerCaughtUp(serverName, serverAddress)
  }

  implicit private class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
