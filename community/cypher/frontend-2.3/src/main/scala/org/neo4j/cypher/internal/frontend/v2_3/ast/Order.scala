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

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheckable}

case class OrderBy(sortItems: Seq[SortItem])(val position: InputPosition) extends ASTNode with ASTPhrase with SemanticCheckable {
  def semanticCheck = sortItems.semanticCheck

  def dependencies: Set[Identifier] =
    sortItems.foldLeft(Set.empty[Identifier]) { case (acc, item) => acc ++ item.expression.dependencies }
}

sealed trait SortItem extends ASTNode with ASTPhrase with SemanticCheckable {
  def expression: Expression
  def semanticCheck = expression.semanticCheck(Expression.SemanticContext.Results)

  def mapExpression(f: Expression => Expression): SortItem
}

case class AscSortItem(expression: Expression)(val position: InputPosition) extends SortItem {
  override def mapExpression(f: Expression => Expression) = copy(expression = f(expression))(position)
}

case class DescSortItem(expression: Expression)(val position: InputPosition) extends SortItem {
  override def mapExpression(f: Expression => Expression) = copy(expression = f(expression))(position)
}

