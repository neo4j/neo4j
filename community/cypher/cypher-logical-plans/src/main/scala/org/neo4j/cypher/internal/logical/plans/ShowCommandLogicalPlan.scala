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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.symbols.CypherType

case class ShowIndexes(
  indexType: ShowIndexType,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW INDEXES"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowIndexes =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowIndexes =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))
}

case class ShowConstraints(
  constraintType: ShowConstraintType,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW CONSTRAINTS"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowConstraints =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowConstraints =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))
}

case class ShowCurrentGraphType(
  asGraph: Boolean,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {

  override def commandDescription: String =
    if (asGraph) "SHOW CURRENT GRAPH TYPE AS GRAPH" else "SHOW CURRENT GRAPH TYPE"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowCurrentGraphType =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowCurrentGraphType =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))
}

case class ShowProcedures(
  executableBy: Option[ExecutableBy],
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW PROCEDURES"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowProcedures =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowProcedures =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))
}

case class ShowFunctions(
  functionType: ShowFunctionType,
  executableBy: Option[ExecutableBy],
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW FUNCTIONS"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowFunctions =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowFunctions =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))
}

case class ShowTransactions(
  ids: CommandClauseNames,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW TRANSACTIONS"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowTransactions =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowTransactions =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))

  override def usedVariables: Set[LogicalVariable] = ids.maybeExpression.map(_.dependencies).getOrElse(Set.empty)
}

case class TerminateTransactions(
  ids: CommandClauseNames,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "TERMINATE TRANSACTIONS"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): TerminateTransactions =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): TerminateTransactions =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))

  override def usedVariables: Set[LogicalVariable] = ids.maybeExpression.map(_.dependencies).getOrElse(Set.empty)
}

case class ShowSettings(
  names: CommandClauseNames,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW SETTINGS"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowSettings =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowSettings =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))

  override def usedVariables: Set[LogicalVariable] = names.maybeExpression.map(_.dependencies).getOrElse(Set.empty)
}

// System database only commands

case class ShowDatabases(
  dbScope: DatabaseScope,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  yieldAll: Boolean,
  columnVariables: Set[LogicalVariable],
  argumentIds: Set[LogicalVariable]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen, argumentIds) {
  override def commandDescription: String = "SHOW DATABASES"

  override def withoutArgumentIds(argsToExclude: Set[LogicalVariable]): ShowDatabases =
    copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def removeArgumentIds(): ShowDatabases =
    copy(argumentIds = Set.empty)(SameId(this.id))

  override def addArgumentIds(argsToAdd: Set[LogicalVariable]): LogicalLeafPlan =
    copy(argumentIds = argumentIds ++ argsToAdd)(SameId(this.id))

  override def usedVariables: Set[LogicalVariable] = dbScope match {
    case SingleNamedDatabaseScope(ParameterName(p)) => p.dependencies
    case _                                          => Set.empty
  }
}

// Column classes

case class CommandDefaultColumn(name: String, cypherType: CypherType)
case class CommandYieldColumn(originalName: String, aliasedName: String)
