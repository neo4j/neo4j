/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.graphdb
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}

import scala.collection.JavaConverters._

object NotificationWrapping {

  def asKernelNotification(offset: Option[InputPosition])(notification: InternalNotification): NotificationCode#Notification = notification match {
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.withOffset(offset).asInputPosition)
    case RuntimeUnsupportedNotification(msg) =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.message("Runtime unsupported", msg))
    case IndexHintUnfulfillableNotification(label, propertyKeys) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.index(label, propertyKeys: _*))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.deprecatedField(procedure, field))
    case DeprecatedVarLengthBindingNotification(pos, variable) =>
      NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.bindingVarLengthRelationship(variable))
    case DeprecatedRelTypeSeparatorNotification(pos) =>
      NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedParameterSyntax(pos) =>
      NotificationCode.DEPRECATED_PARAMETER_SYNTAX.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedCreateIndexSyntax(pos) =>
      NotificationCode.DEPRECATED_CREATE_INDEX_SYNTAX.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedDropIndexSyntax(pos) =>
      NotificationCode.DEPRECATED_DROP_INDEX_SYNTAX.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedDropConstraintSyntax(pos) =>
      NotificationCode.DEPRECATED_DROP_CONSTRAINT_SYNTAX.notification(pos.withOffset(offset).asInputPosition)
    case DeprecatedCompiledRuntimeNotification =>
      NotificationCode.DEPRECATED_COMPILED_RUNTIME.notification(graphdb.InputPosition.empty)
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCode.PROCEDURE_WARNING.notification(pos.withOffset(offset).asInputPosition, NotificationDetail.Factory.procedureWarning(name, warning))
    case ExperimentalFeatureNotification(msg) =>
      NotificationCode.EXPERIMENTAL_FEATURE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.message("PARALLEL", msg))
    case SuboptimalIndexForConstainsQueryNotification(label, properties) =>
      NotificationCode.SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.suboptimalIndex(label, properties: _*))
    case SuboptimalIndexForEndsWithQueryNotification(label, properties) =>
      NotificationCode.SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.suboptimalIndex(label, properties: _*))
    case MissingParametersNotification(names) =>
      NotificationCode.MISSING_PARAMETERS_FOR_EXPLAIN.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.message("Explain with missing parameters", names.mkString("Missing parameters: ",", ","")))
    case CodeGenerationFailedNotification(msg) =>
      NotificationCode.CODE_GENERATION_FAILED.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.message("Error from code generation", msg))
  }

  private implicit class ConvertibleCompilerInputPosition(pos: InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
