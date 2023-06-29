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
package org.neo4j.cypher.internal

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.compiler.CodeGenerationFailedNotification
import org.neo4j.cypher.internal.compiler.DeprecatedFieldNotification
import org.neo4j.cypher.internal.compiler.DeprecatedProcedureNotification
import org.neo4j.cypher.internal.compiler.EagerLoadCsvNotification
import org.neo4j.cypher.internal.compiler.ExhaustiveShortestPathForbiddenNotification
import org.neo4j.cypher.internal.compiler.ExperimentalFeatureNotification
import org.neo4j.cypher.internal.compiler.IndexHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.JoinHintUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.LargeLabelWithLoadCsvNotification
import org.neo4j.cypher.internal.compiler.MissingLabelNotification
import org.neo4j.cypher.internal.compiler.MissingParametersNotification
import org.neo4j.cypher.internal.compiler.MissingPropertyNameNotification
import org.neo4j.cypher.internal.compiler.MissingRelTypeNotification
import org.neo4j.cypher.internal.compiler.NodeIndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.ProcedureWarningNotification
import org.neo4j.cypher.internal.compiler.RelationshipIndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.util.AssignPrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.util.DeprecatedDatabaseNameNotification
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
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
import org.neo4j.cypher.internal.util.RepeatedRelationshipReference
import org.neo4j.cypher.internal.util.RepeatedVarLengthRelationshipReference
import org.neo4j.cypher.internal.util.RevokePrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.UnionReturnItemsInDifferentOrder
import org.neo4j.cypher.internal.util.UnsatisfiableRelationshipTypeExpression
import org.neo4j.graphdb
import org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription
import org.neo4j.graphdb.impl.notification.NotificationDetail
import org.neo4j.graphdb.impl.notification.NotificationImplementation

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

object NotificationWrapping {

  def asKernelNotification(offset: Option[InputPosition])(notification: InternalNotification)
    : NotificationImplementation = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCodeWithDescription.cartesianProduct(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.cartesianProductDescription(variables.asJava)
      )
    case RuntimeUnsupportedNotification(msg) =>
      NotificationCodeWithDescription.runtimeUnsupported(
        graphdb.InputPosition.empty,
        msg
      )
    case IndexHintUnfulfillableNotification(variableName, label, propertyKeys, entityType, indexType) =>
      val detail = entityType match {
        case EntityType.NODE => indexType match {
            case UsingAnyIndexType   => NotificationDetail.nodeAnyIndex(variableName, label, propertyKeys: _*)
            case UsingTextIndexType  => NotificationDetail.nodeTextIndex(variableName, label, propertyKeys: _*)
            case UsingRangeIndexType => NotificationDetail.nodeRangeIndex(variableName, label, propertyKeys: _*)
            case UsingPointIndexType => NotificationDetail.nodePointIndex(variableName, label, propertyKeys: _*)
          }
        case EntityType.RELATIONSHIP => indexType match {
            case UsingAnyIndexType =>
              NotificationDetail.relationshipAnyIndex(variableName, label, propertyKeys: _*)
            case UsingTextIndexType =>
              NotificationDetail.relationshipTextIndex(variableName, label, propertyKeys: _*)
            case UsingRangeIndexType =>
              NotificationDetail.relationshipRangeIndex(variableName, label, propertyKeys: _*)
            case UsingPointIndexType =>
              NotificationDetail.relationshipPointIndex(variableName, label, propertyKeys: _*)
          }
      }
      NotificationCodeWithDescription.indexHintUnfulfillable(graphdb.InputPosition.empty, detail)
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCodeWithDescription.joinHintUnfulfillable(
        graphdb.InputPosition.empty,
        NotificationDetail.joinKey(variables.asJava)
      )
    case NodeIndexLookupUnfulfillableNotification(labels) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.nodeIndexSeekOrScan(labels.asJava)
      )
    case RelationshipIndexLookupUnfulfillableNotification(relTypes) =>
      NotificationCodeWithDescription.indexLookupForDynamicProperty(
        graphdb.InputPosition.empty,
        NotificationDetail.relationshipIndexSeekOrScan(relTypes.asJava)
      )
    case EagerLoadCsvNotification =>
      NotificationCodeWithDescription.eagerLoadCsv(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCodeWithDescription.largeLabelLoadCsv(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCodeWithDescription.missingLabel(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.label(label)
      )
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCodeWithDescription.missingRelType(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.relationshipType(relType)
      )
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCodeWithDescription.missingPropertyName(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.propertyName(name)
      )
    case UnboundedShortestPathNotification(pos) =>
      NotificationCodeWithDescription.unboundedShortestPath(pos.withOffset(offset).asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCodeWithDescription.exhaustiveShortestPath(pos.withOffset(offset).asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCodeWithDescription.deprecatedFunction(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedName(oldName, newName)
      )
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCodeWithDescription.deprecatedProcedure(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedName(oldName, newName)
      )
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCodeWithDescription.deprecatedProcedureReturnField(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecatedField(procedure, field)
      )
    case DeprecatedRelTypeSeparatorNotification(pos, rewrittenExpression) =>
      NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.deprecationNotificationDetail(rewrittenExpression)
      )
    case DeprecatedNodesOrRelationshipsInSetClauseNotification(pos) =>
      NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause(
        pos.withOffset(offset).asInputPosition
      )
    case DeprecatedPropertyReferenceInCreate(pos, name) =>
      NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate(
        pos.withOffset(offset).asInputPosition,
        name
      )
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCodeWithDescription.procedureWarning(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.procedureWarning(name, warning)
      )
    case ExperimentalFeatureNotification(msg) =>
      NotificationCodeWithDescription.runtimeExperimental(
        graphdb.InputPosition.empty,
        msg
      )
    case MissingParametersNotification(names) =>
      NotificationCodeWithDescription.missingParameterForExplain(
        graphdb.InputPosition.empty,
        names.mkString("Missing parameters: ", ", ", "")
      )
    case CodeGenerationFailedNotification(msg) =>
      NotificationCodeWithDescription.codeGenerationFailed(
        graphdb.InputPosition.empty,
        msg
      )
    case SubqueryVariableShadowing(pos, varName) =>
      NotificationCodeWithDescription.subqueryVariableShadowing(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.shadowingVariable(varName)
      )
    case UnionReturnItemsInDifferentOrder(pos) =>
      NotificationCodeWithDescription.unionReturnOrder(
        pos.withOffset(offset).asInputPosition
      )
    case HomeDatabaseNotPresent(name) => NotificationCodeWithDescription.homeDatabaseNotPresent(
        InputPosition.NONE.asInputPosition,
        s"HOME DATABASE: $name"
      )
    case FixedLengthRelationshipInShortestPath(pos) =>
      NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship(
        pos.withOffset(offset).asInputPosition
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

    case DeprecatedRuntimeNotification(msg) =>
      NotificationCodeWithDescription.deprecatedRuntimeOption(
        graphdb.InputPosition.empty,
        msg
      )

    case UnsatisfiableRelationshipTypeExpression(position, relTypeExpression) =>
      NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.unsatisfiableRelTypeExpression(relTypeExpression)
      )

    case RepeatedRelationshipReference(position, relName) =>
      NotificationCodeWithDescription.repeatedRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedRelationship(relName)
      )

    case RepeatedVarLengthRelationshipReference(position, relName) =>
      NotificationCodeWithDescription.repeatedVarLengthRelationshipReference(
        position.withOffset(offset).asInputPosition,
        NotificationDetail.repeatedVarLengthRel(relName)
      )

    case DeprecatedConnectComponentsPlannerPreParserOption(position) =>
      // Not using .withOffset(offset) is intentional.
      // This notification is generated from the pre-parser and thus should not be offset.
      NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption(
        position.asInputPosition
      )

    case AssignPrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffect(
        graphdb.InputPosition.empty,
        command,
        "The role already has the privilege."
      )

    case RevokePrivilegeCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffect(
        graphdb.InputPosition.empty,
        command,
        "The role does not have the privilege."
      )

    case GrantRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffect(
        graphdb.InputPosition.empty,
        command,
        "The user already has the role."
      )

    case RevokeRoleCommandHasNoEffectNotification(command) =>
      NotificationCodeWithDescription.commandHasNoEffect(
        graphdb.InputPosition.empty,
        command,
        "The user does not have the role."
      )

    case ImpossibleRevokeCommandWarning(command, cause) =>
      NotificationCodeWithDescription.impossibleRevokeCommand(
        graphdb.InputPosition.empty,
        command,
        cause
      )

    case _ => throw new IllegalStateException("Missing mapping for notification detail.")
  }

  implicit private class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
