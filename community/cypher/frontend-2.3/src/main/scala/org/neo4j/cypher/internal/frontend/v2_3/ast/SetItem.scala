/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheckable}

sealed trait SetItem extends ASTNode with ASTPhrase with SemanticCheckable

case class SetLabelItem(expression: Expression, labels: Seq[LabelName])(val position: InputPosition) extends SetItem {
  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
    expression.expectType(CTNode.covariant)
}

sealed trait SetProperty extends SetItem

case class SetPropertyItem(property: Property, expression: Expression)(val position: InputPosition) extends SetProperty {
  def semanticCheck =
    property.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      property.map.expectType(CTNode.covariant | CTRelationship.covariant)
}

case class SetExactPropertiesFromMapItem(identifier: Identifier, expression: Expression)
                                        (val position: InputPosition) extends SetProperty {
  def semanticCheck =
    identifier.semanticCheck(Expression.SemanticContext.Simple) chain
    identifier.expectType(CTNode.covariant | CTRelationship.covariant) chain
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
    expression.expectType(CTMap.covariant)
}

case class SetIncludingPropertiesFromMapItem(identifier: Identifier, expression: Expression)
                                        (val position: InputPosition) extends SetProperty {
  def semanticCheck =
    identifier.semanticCheck(Expression.SemanticContext.Simple) chain
    identifier.expectType(CTNode.covariant | CTRelationship.covariant) chain
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
    expression.expectType(CTMap.covariant)
}
