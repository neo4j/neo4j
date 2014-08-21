/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_2.ast.{Expression, Parameter}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EntityByIdRhs => CommandEntityByIdRhs, EntityByIdParameter => CommandEntityByIdParameter, EntityByIdExprs => CommandEntityByIdExprs}
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.ExpressionConverters._

sealed trait EntityByIdRhs {
  def asEntityByIdRhs: CommandEntityByIdRhs
}
case class EntityByIdParameter(parameter: Parameter) extends EntityByIdRhs {
  def asEntityByIdRhs =
    CommandEntityByIdParameter(parameter.asCommandParameter)
}
case class EntityByIdExprs(exprs: Seq[Expression]) extends EntityByIdRhs {
  def asEntityByIdRhs =
    CommandEntityByIdExprs(exprs.asCommandExpressions)
}

case class NodeByIdSeek(idName: IdName, nodeIds: EntityByIdRhs, argumentIds: Set[IdName]) extends LogicalLeafPlan {
  def availableSymbols: Set[IdName] = argumentIds + idName
}
