/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.attribution.IdGen

case class ShowIndexes(indexType: ShowIndexType, verbose: Boolean, defaultColumns: List[ShowColumn])(implicit
idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowConstraints(constraintType: ShowConstraintType, verbose: Boolean, defaultColumns: List[ShowColumn])(
  implicit idGen: IdGen
) extends CommandLogicalPlan(idGen)

case class ShowProcedures(executableBy: Option[ExecutableBy], verbose: Boolean, defaultColumns: List[ShowColumn])(
  implicit idGen: IdGen
) extends CommandLogicalPlan(idGen)

case class ShowFunctions(
  functionType: ShowFunctionType,
  executableBy: Option[ExecutableBy],
  verbose: Boolean,
  defaultColumns: List[ShowColumn]
)(implicit idGen: IdGen) extends CommandLogicalPlan(idGen)

case class ShowTransactions(ids: Either[List[String], Parameter], verbose: Boolean, defaultColumns: List[ShowColumn])(
  implicit idGen: IdGen
) extends CommandLogicalPlan(idGen)

case class TerminateTransactions(ids: Either[List[String], Parameter], defaultColumns: List[ShowColumn])(implicit
idGen: IdGen) extends CommandLogicalPlan(idGen)
