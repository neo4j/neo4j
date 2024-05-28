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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlanExtension
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

case class FakeLeafPlan(argumentIdStrings: Set[String] = Set.empty)(implicit idGen: IdGen)
    extends LogicalLeafPlanExtension(idGen) {
  override val argumentIds: Set[LogicalVariable] = argumentIdStrings.map(varFor)
  override val availableSymbols: Set[LogicalVariable] = argumentIds

  override def usedVariables: Set[LogicalVariable] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIdStrings = (argumentIds -- argsToExclude).map(_.name))(SameId(this.id))

  override def removeArgumentIds(): LogicalLeafPlan =
    copy(argumentIdStrings = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIdStrings = (argumentIds ++ argsToAdd).map(_.name))(SameId(this.id))
}
