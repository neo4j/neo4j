/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.notification

import org.neo4j.cypher.internal.frontend.v3_3.InputPosition

/**
 * Describes a notification
 */
sealed trait InternalNotification

case class DeprecatedStartNotification(position: InputPosition, alternativeQuery: String) extends InternalNotification

case class CartesianProductNotification(position: InputPosition, isolatedVariables: Set[String]) extends InternalNotification

case class LengthOnNonPathNotification(position: InputPosition) extends InternalNotification

case object PlannerUnsupportedNotification extends InternalNotification

case object RuntimeUnsupportedNotification extends InternalNotification

case class IndexHintUnfulfillableNotification(label: String, propertyKeys: Seq[String]) extends InternalNotification

case class JoinHintUnfulfillableNotification(identified: Seq[String]) extends InternalNotification

case class JoinHintUnsupportedNotification(identified: Seq[String]) extends InternalNotification

case class IndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case object EagerLoadCsvNotification extends InternalNotification

case object LargeLabelWithLoadCsvNotification extends InternalNotification

case class MissingLabelNotification(position: InputPosition, label: String) extends InternalNotification

case class MissingRelTypeNotification(position: InputPosition, relType: String) extends InternalNotification

case class MissingPropertyNameNotification(position: InputPosition, name: String) extends InternalNotification

case class UnboundedShortestPathNotification(position: InputPosition) extends InternalNotification

case class ExhaustiveShortestPathForbiddenNotification(position: InputPosition) extends InternalNotification

case class DeprecatedFunctionNotification(position: InputPosition, oldName: String, newName: String) extends InternalNotification

case class DeprecatedProcedureNotification(position: InputPosition, oldName: String, newName: String) extends InternalNotification

case class ProcedureWarningNotification(position: InputPosition, procedure: String, warning: String) extends InternalNotification

case class DeprecatedFieldNotification(position: InputPosition, procedure: String, field: String) extends InternalNotification

case class DeprecatedVarLengthBindingNotification(position: InputPosition, variable: String) extends InternalNotification

case class DeprecatedRelTypeSeparatorNotification(position: InputPosition) extends InternalNotification

case object DeprecatedPlannerNotification extends InternalNotification
