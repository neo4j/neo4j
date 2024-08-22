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

import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.IdGen

case class ShowIndexes(
  indexType: ShowIndexType,
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowConstraints(
  constraintType: ShowConstraintType,
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean,
  returnCypher5Values: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowProcedures(
  executableBy: Option[ExecutableBy],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowFunctions(
  functionType: ShowFunctionType,
  executableBy: Option[ExecutableBy],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowTransactions(
  ids: Either[List[String], Expression],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class TerminateTransactions(
  ids: Either[List[String], Expression],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowSettings(
  names: Either[List[String], Expression],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  yieldAll: Boolean
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)
