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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._

sealed trait SetItem extends AstNode with SemanticCheckable

case class SetPropertyItem(property: Property, expression: Expression)(val token: InputToken) extends SetItem {
  def semanticCheck =
    property.semanticCheck(Expression.SemanticContext.Simple) then
    expression.semanticCheck(Expression.SemanticContext.Simple)
}

case class SetLabelItem(expression: Expression, labels: Seq[Identifier])(val token: InputToken) extends SetItem {
  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) then
    expression.expectType(CTNode.covariant)
}

case class SetPropertiesFromMapItem(identifier: Identifier, expression: Expression)(val token: InputToken) extends SetItem {
  def semanticCheck =
    identifier.semanticCheck(Expression.SemanticContext.Simple) then
    identifier.expectType(CTNode.covariant | CTRelationship.covariant) then
    expression.semanticCheck(Expression.SemanticContext.Simple) then
    expression.expectType(CTMap.covariant)
}
