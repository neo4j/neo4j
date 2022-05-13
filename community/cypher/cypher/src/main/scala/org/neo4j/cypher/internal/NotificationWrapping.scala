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
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForConstainsQueryNotification
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForEndsWithQueryNotification
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedAmbiguousGroupingNotification
import org.neo4j.cypher.internal.util.DeprecatedCoercionOfListToBoolean
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.graphdb
import org.neo4j.graphdb.impl.notification.NotificationCode
import org.neo4j.graphdb.impl.notification.NotificationDetail

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

object NotificationWrapping {

  def asKernelNotification(offset: Option[InputPosition])(notification: InternalNotification)
    : NotificationCode#Notification = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.cartesianProduct(variables.asJava)
      )
    case RuntimeUnsupportedNotification(msg) =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.message("Runtime unsupported", msg)
      )
    case IndexHintUnfulfillableNotification(variableName, label, propertyKeys, entityType, indexType) =>
      val detail = entityType match {
        case EntityType.NODE => indexType match {
            case UsingAnyIndexType   => NotificationDetail.Factory.nodeAnyIndex(variableName, label, propertyKeys: _*)
            case UsingTextIndexType  => NotificationDetail.Factory.nodeTextIndex(variableName, label, propertyKeys: _*)
            case UsingRangeIndexType => NotificationDetail.Factory.nodeRangeIndex(variableName, label, propertyKeys: _*)
            case UsingPointIndexType => NotificationDetail.Factory.nodePointIndex(variableName, label, propertyKeys: _*)
          }
        case EntityType.RELATIONSHIP => indexType match {
            case UsingAnyIndexType =>
              NotificationDetail.Factory.relationshipAnyIndex(variableName, label, propertyKeys: _*)
            case UsingTextIndexType =>
              NotificationDetail.Factory.relationshipTextIndex(variableName, label, propertyKeys: _*)
            case UsingRangeIndexType =>
              NotificationDetail.Factory.relationshipRangeIndex(variableName, label, propertyKeys: _*)
            case UsingPointIndexType =>
              NotificationDetail.Factory.relationshipPointIndex(variableName, label, propertyKeys: _*)
          }
      }
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, detail)
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.joinKey(variables.asJava)
      )
    case NodeIndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.nodeIndexSeekOrScan(labels.asJava)
      )
    case RelationshipIndexLookupUnfulfillableNotification(relTypes) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.relationshipIndexSeekOrScan(relTypes.asJava)
      )
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.label(label)
      )
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.relationshipType(relType)
      )
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.propertyName(name)
      )
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.deprecatedName(oldName, newName)
      )
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.deprecatedName(oldName, newName)
      )
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.deprecatedField(procedure, field)
      )
    case DeprecatedRelTypeSeparatorNotification(pos) =>
      NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(pos.withOffset(offset).asInputPosition)
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCode.PROCEDURE_WARNING.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.procedureWarning(name, warning)
      )
    case ExperimentalFeatureNotification(msg) =>
      NotificationCode.EXPERIMENTAL_FEATURE.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.message("PARALLEL", msg)
      )
    case SuboptimalIndexForConstainsQueryNotification(variableName, label, propertyKeys, entityType) =>
      val detail = entityType match {
        case EntityType.NODE => NotificationDetail.Factory.nodeAnyIndex(variableName, label, propertyKeys: _*)
        case EntityType.RELATIONSHIP =>
          NotificationDetail.Factory.relationshipAnyIndex(variableName, label, propertyKeys: _*)
      }
      NotificationCode.SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, detail)
    case SuboptimalIndexForEndsWithQueryNotification(variableName, label, propertyKeys, entityType) =>
      val detail = entityType match {
        case EntityType.NODE => NotificationDetail.Factory.nodeAnyIndex(variableName, label, propertyKeys: _*)
        case EntityType.RELATIONSHIP =>
          NotificationDetail.Factory.relationshipAnyIndex(variableName, label, propertyKeys: _*)
      }
      NotificationCode.SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, detail)
    case MissingParametersNotification(names) =>
      NotificationCode.MISSING_PARAMETERS_FOR_EXPLAIN.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.message(
          "Explain with missing parameters",
          names.mkString("Missing parameters: ", ", ", "")
        )
      )
    case CodeGenerationFailedNotification(msg) =>
      NotificationCode.CODE_GENERATION_FAILED.notification(
        graphdb.InputPosition.empty,
        NotificationDetail.Factory.message("Error from code generation", msg)
      )
    case DeprecatedCoercionOfListToBoolean(pos) =>
      NotificationCode.DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN.notification(pos.withOffset(offset).asInputPosition)
    case SubqueryVariableShadowing(pos, varName) =>
      NotificationCode.SUBQUERY_VARIABLE_SHADOWING.notification(
        pos.withOffset(offset).asInputPosition,
        NotificationDetail.Factory.shadowingVariable(varName)
      )
    case DeprecatedAmbiguousGroupingNotification(pos, maybeHint) =>
      maybeHint match {
        case Some(hint) => NotificationCode.DEPRECATED_AMBIGUOUS_GROUPING_NOTIFICATION
            .notification(
              pos.withOffset(offset).asInputPosition,
              NotificationDetail.Factory.message("Hint", s"Hint: $hint")
            )
        case _ => NotificationCode.DEPRECATED_AMBIGUOUS_GROUPING_NOTIFICATION.notification(
            pos.withOffset(offset).asInputPosition
          )
      }
    case _ => throw new IllegalStateException("Missing mapping for notification detail.")
  }

  implicit private class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
