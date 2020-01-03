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
package org.neo4j.cypher.internal.compiler.v3_5

import org.neo4j.cypher.internal.v3_5.util.{InputPosition, InternalNotification}

case class SuboptimalIndexForConstainsQueryNotification(label: String, propertyKeys: Seq[String]) extends InternalNotification

case class SuboptimalIndexForEndsWithQueryNotification(label: String, propertyKeys: Seq[String]) extends InternalNotification

case object StartUnavailableFallback extends InternalNotification

case class CreateUniqueUnavailableFallback(position: InputPosition) extends InternalNotification

case class CreateUniqueDeprecated(position: InputPosition) extends InternalNotification

case object RulePlannerUnavailableFallbackNotification extends InternalNotification

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

case class ExhaustiveShortestPathForbiddenNotification(position: InputPosition) extends InternalNotification

case class DeprecatedProcedureNotification(position: InputPosition, oldName: String, newName: String) extends InternalNotification

case class ProcedureWarningNotification(position: InputPosition, procedure: String, warning: String) extends InternalNotification

case class DeprecatedFieldNotification(position: InputPosition, procedure: String, field: String) extends InternalNotification

case object DeprecatedRulePlannerNotification extends InternalNotification

case object DeprecatedCompiledRuntimeNotification extends InternalNotification

case class ExperimentalFeatureNotification(msg: String) extends InternalNotification
