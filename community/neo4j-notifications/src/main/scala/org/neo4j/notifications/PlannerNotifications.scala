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

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintType
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification

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
  indexType: UsingIndexHintType
) extends InternalNotification

case class JoinHintUnfulfillableNotification(identified: Seq[String]) extends InternalNotification

case class NodeIndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case class RelationshipIndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case object EagerLoadCsvNotification extends InternalNotification

case class LargeLabelWithLoadCsvNotification(labelName: String) extends InternalNotification

case class MissingLabelNotification(position: InputPosition, label: String) extends InternalNotification

case class MissingRelTypeNotification(position: InputPosition, relType: String) extends InternalNotification

case class MissingPropertyNameNotification(position: InputPosition, name: String) extends InternalNotification

case class ExhaustiveShortestPathForbiddenNotification(position: InputPosition, pathPredicates: Set[String])
    extends InternalNotification

case class DeprecatedProcedureNotification(position: InputPosition, oldName: String, newName: String)
    extends InternalNotification

case class ProcedureWarningNotification(position: InputPosition, procedure: String, warning: String)
    extends InternalNotification

case class DeprecatedProcedureReturnFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class DeprecatedProcedureFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class DeprecatedFunctionFieldNotification(position: InputPosition, procedure: String, field: String)
    extends InternalNotification

case class MissingParametersNotification(parameters: Seq[String]) extends InternalNotification

case class CodeGenerationFailedNotification(
  failingRuntimeConf: String,
  fallbackRuntimeConf: String,
  msg: String
) extends InternalNotification
