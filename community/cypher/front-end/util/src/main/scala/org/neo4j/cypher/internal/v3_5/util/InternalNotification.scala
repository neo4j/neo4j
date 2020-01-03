/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.util

/**
 * Describes a notification
 */
trait InternalNotification

case class DeprecatedStartNotification(position: InputPosition, alternativeQuery: String) extends InternalNotification

case class CartesianProductNotification(position: InputPosition, isolatedVariables: Set[String]) extends InternalNotification

case class LengthOnNonPathNotification(position: InputPosition) extends InternalNotification

case class UnboundedShortestPathNotification(position: InputPosition) extends InternalNotification

case class DeprecatedFunctionNotification(position: InputPosition, oldName: String, newName: String) extends InternalNotification

case class DeprecatedVarLengthBindingNotification(position: InputPosition, variable: String) extends InternalNotification

case class DeprecatedRelTypeSeparatorNotification(position: InputPosition) extends InternalNotification

case class DeprecatedParameterSyntax(position: InputPosition) extends InternalNotification
