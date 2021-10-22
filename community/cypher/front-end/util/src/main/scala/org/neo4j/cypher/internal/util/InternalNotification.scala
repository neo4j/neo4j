/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.util

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

case class DeprecatedCreateIndexSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedBtreeIndexSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedDropIndexSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedDropConstraintSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedCreatePropertyExistenceConstraintSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedCreateConstraintOnAssertSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedShowSchemaSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedShowExistenceConstraintSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedPropertyExistenceSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedRepeatedRelVarInPatternExpression(position: InputPosition, relName: String) extends InternalNotification

case class DeprecatedOctalLiteralSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedHexLiteralSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedPatternExpressionOutsideExistsSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedDefaultDatabaseSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedDefaultGraphSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedCatalogKeywordForAdminCommandSyntax(position: InputPosition) extends InternalNotification

case class DeprecatedPeriodicCommit(position: InputPosition) extends InternalNotification

case class DeprecatedCoercionOfListToBoolean(position: InputPosition) extends InternalNotification

case class DeprecatedSelfReferenceToVariableInCreatePattern(position: InputPosition) extends InternalNotification

case class DeprecatedPointsComparison(position: InputPosition) extends InternalNotification

case class SubqueryVariableShadowing(position: InputPosition, varName: String) extends InternalNotification

case class MissingAliasNotification(pos: InputPosition) extends InternalNotification

case class DeprecatedAmbiguousGroupingNotification(pos: InputPosition, hint: Option[String]) extends InternalNotification