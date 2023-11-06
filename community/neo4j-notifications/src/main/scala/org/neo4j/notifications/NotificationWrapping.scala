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
package org.neo4j.notifications

import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.util.AssignPrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.CordonedServersExistedDuringAllocation
import org.neo4j.cypher.internal.util.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedIdentifierUnicode
import org.neo4j.cypher.internal.util.DeprecatedIdentifierWhitespaceUnicode
import org.neo4j.cypher.internal.util.DeprecatedNodesOrRelationshipsInSetClauseNotification
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.DeprecatedRuntimeNotification
import org.neo4j.cypher.internal.util.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.util.FixedLengthRelationshipInShortestPath
import org.neo4j.cypher.internal.util.GrantRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.HomeDatabaseNotPresent
import org.neo4j.cypher.internal.util.ImpossibleRevokeCommandWarning
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.NoDatabasesReallocated
import org.neo4j.cypher.internal.util.RepeatedRelationshipReference
import org.neo4j.cypher.internal.util.RepeatedVarLengthRelationshipReference
import org.neo4j.cypher.internal.util.RevokePrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.ServerAlreadyCordoned
import org.neo4j.cypher.internal.util.ServerAlreadyEnabled
import org.neo4j.cypher.internal.util.SideEffectVisibility
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.UnionReturnItemsInDifferentOrder
import org.neo4j.cypher.internal.util.UnsatisfiableRelationshipTypeExpression
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType
import org.neo4j.graphdb

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

object NotificationWrapping {

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
      val indexHintType = indexType match {
        case UsingAnyIndexType   => IndexHintIndexType.ANY
        case UsingTextIndexType  => IndexHintIndexType.TEXT
        case UsingRangeIndexType => IndexHintIndexType.RANGE
        case UsingPointIndexType => IndexHintIndexType.POINT
      }
      NotificationCodeWithDescription.indexHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.indexHint(entityType, indexHintType, variableName, label, propertyKeys: _*),
        NotificationDetail.index(indexHintType, label, propertyKeys.asJava)
      )
    case JoinHintUnfulfillableNotification(variables) =>
      val javaVariables = variables.asJava
      NotificationCodeWithDescription.joinHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.joinKey(javaVariables),
        NotificationDetail.commaSeparated(javaVariables)
      )
    case NodeIndexLookupUnfulfillableNotification(labels) =>
      val javaLabels = labels.asJava
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.nodeIndexSeekOrScan(javaLabels),
        NotificationDetail.commaSeparated(javaLabels)
      )
    case RelationshipIndexLookupUnfulfillableNotification(relTypes) =>
      val javaRelTypes = relTypes.asJava
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.relationshipIndexSeekOrScan(javaRelTypes),
        NotificationDetail.commaSeparated(javaRelTypes)
      )
    case EagerLoadCsvNotification =>
      NotificationCodeWithDescription.eagerLoadCsv(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification(labelName) =>
      NotificationCodeWithDescription.largeLabelLoadCsv(
        graphdb.InputPosition.empty,
        labelName
      )
    case MissingLabelNotification(pos, label) =>
      NotificationCodeWithDescription.missingLabel(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingLabel(label),
        label
      )
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCodeWithDescription.missingRelType(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.missingRelationshipType(relType),
        relType
      )
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCodeWithDescription.missingPropertyName(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.propertyName(name),
        name
      )
    case UnboundedShortestPathNotification(pos, pattern) =>
      NotificationCodeWithDescription.unboundedShortestPath(pos.withOffset(offset).asInputPosition, pattern)
    case ExhaustiveShortestPathForbiddenNotification(pos, pathPredicates) =>
      NotificationCodeWithDescription.exhaustiveShortestPath(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.commaSeparated(pathPredicates.asJava)
      )
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      if (newName == null || newName.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedFunctionWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName),
          oldName,
          newName
        )
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      if (newName == null || newName.trim.isEmpty)
        NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName),
          oldName
        )
      else
        NotificationCodeWithDescription.deprecatedProcedureWithReplacement(
          pos.withOffset(offset).asInputPosition,
          NotificationDetail.deprecatedName(oldName, newName),
          oldName,
          newName
        )
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureReturnField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedField(procedure, field),
        procedure,
        field
      )
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
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCodeWithDescription.procedureWarning(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.procedureWarning(name, warning),
        warning,
        name
      )
    case MissingParametersNotification(parameters) =>
      val javaParameters = parameters.asJava
      NotificationCodeWithDescription.missingParameterForExplain(
        graphdb.InputPosition.empty,
        NotificationDetail.missingParameters(javaParameters),
        NotificationDetail.parameters(javaParameters)
      )
    case CodeGenerationFailedNotification(failingConf, fallbackRuntimeConf, cause) =>
      NotificationCodeWithDescription.codeGenerationFailed(
        graphdb.InputPosition.empty,
        failingConf,
        fallbackRuntimeConf,
        cause
      )
    case SubqueryVariableShadowing(pos, varName) =>
      NotificationCodeWithDescription.subqueryVariableShadowing(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.shadowingVariable(varName),
        varName
      )
    case UnionReturnItemsInDifferentOrder(pos) =>
      NotificationCodeWithDescription.unionReturnOrder(
        pos.withOffset(offset).asInputPosition
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

    case DeprecatedDatabaseNameNotification(name, pos) =>
      NotificationCodeWithDescription.deprecatedDatabaseName(
        pos.map(_.withOffset(offset).asInputPosition).getOrElse(graphdb.InputPosition.empty),
        s"Name: $name"
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

    case DeprecatedIdentifierWhitespaceUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode(
        position.asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedIdentifierUnicode(position, unicode, identifier) =>
      NotificationCodeWithDescription.deprecatedIdentifierUnicode(
        position.asInputPosition,
        unicode,
        identifier
      )

    case DeprecatedConnectComponentsPlannerPreParserOption(position) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption(
        position.asInputPosition
      )

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

    case SideEffectVisibility(position) =>
      NotificationCodeWithDescription.sideEffectVisibility(
        position.withOffset(offset).asInputPosition
      )

    case CordonedServersExistedDuringAllocation(servers) =>
      NotificationCodeWithDescription.cordonedServersExist(
        graphdb.InputPosition.empty,
        servers
      )

    case _ => throw new IllegalStateException("Missing mapping for notification detail.")
  }

  implicit private class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
