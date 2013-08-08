/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0.ast

import org.neo4j.cypher.internal.parser.v2_0._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.mutation

sealed trait SetItem extends AstNode with SemanticCheckable {
  def toLegacyUpdateAction : mutation.UpdateAction
}

case class SetPropertyItem(property: Property, expression: Expression, token: InputToken) extends SetItem {
  def semanticCheck = Seq(property, expression).semanticCheck(Expression.SemanticContext.Simple)

  def toLegacyUpdateAction = mutation.PropertySetAction(property.toCommand, expression.toCommand)
}

case class SetLabelItem(expression: Expression, labels: Seq[Identifier], token: InputToken) extends SetItem {
  def semanticCheck = {
    expression.semanticCheck(Expression.SemanticContext.Simple) then
      expression.limitType(NodeType())
  }

  def toLegacyUpdateAction = {
    commands.LabelAction(expression.toCommand, commands.LabelSetOp, labels.map(l => commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)))
  }
}

case class SetNodeItem(identifier: Identifier, expression: Expression, token: InputToken) extends SetItem {
  def semanticCheck = {
    Seq(identifier, expression).semanticCheck(Expression.SemanticContext.Simple) then
      identifier.limitType(NodeType()) then
      expression.limitType(MapType())
  }

  def toLegacyUpdateAction = mutation.MapPropertySetAction(commandexpressions.Identifier(identifier.name), expression.toCommand)
}
