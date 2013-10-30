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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.commands
import org.neo4j.cypher.internal.compiler.v2_0.commands.{values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_0.mutation

sealed trait RemoveItem extends AstNode with SemanticCheckable {
  def toLegacyUpdateAction : mutation.UpdateAction
}

case class RemoveLabelItem(expression: Expression, labels: Seq[Identifier], token: InputToken) extends RemoveItem {
  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
    expression.constrainType(NodeType())

  def toLegacyUpdateAction =
    commands.LabelAction(expression.toCommand, commands.LabelRemoveOp, labels.map(l => commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)))
}

case class RemovePropertyItem(property: Property) extends RemoveItem {
  def token = property.token

  def semanticCheck = property.semanticCheck(Expression.SemanticContext.Simple)

  def toLegacyUpdateAction =
    mutation.DeletePropertyAction(property.map.toCommand, commandvalues.KeyToken.Unresolved(property.identifier.name, commandvalues.TokenType.PropertyKey))
}
