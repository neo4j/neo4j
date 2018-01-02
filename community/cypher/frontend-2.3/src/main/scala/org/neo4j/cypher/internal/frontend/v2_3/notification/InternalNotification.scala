/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v2_3.notification

import org.neo4j.cypher.internal.frontend.v2_3.InputPosition

/**
 * Describes a notification
 */
sealed trait InternalNotification

case class CartesianProductNotification(position: InputPosition, isolatedIdentifiers: Set[String]) extends InternalNotification

case class LengthOnNonPathNotification(position: InputPosition) extends InternalNotification

case object LegacyPlannerNotification extends InternalNotification

case object PlannerUnsupportedNotification extends InternalNotification

case object RuntimeUnsupportedNotification extends InternalNotification

case class IndexHintUnfulfillableNotification(label: String, propertyKey: String) extends InternalNotification

case class JoinHintUnfulfillableNotification(identified: Seq[String]) extends InternalNotification

case class JoinHintUnsupportedNotification(identified: Seq[String]) extends InternalNotification

case class IndexLookupUnfulfillableNotification(labels: Set[String]) extends InternalNotification

case class BareNodeSyntaxDeprecatedNotification(position: InputPosition) extends InternalNotification

case object EagerLoadCsvNotification extends InternalNotification

case object LargeLabelWithLoadCsvNotification extends InternalNotification

case class MissingLabelNotification(position: InputPosition, label: String) extends InternalNotification

case class MissingRelTypeNotification(position: InputPosition, relType: String) extends InternalNotification

case class MissingPropertyNameNotification(position: InputPosition, name: String) extends InternalNotification

case class UnboundedShortestPathNotification(position: InputPosition) extends InternalNotification
