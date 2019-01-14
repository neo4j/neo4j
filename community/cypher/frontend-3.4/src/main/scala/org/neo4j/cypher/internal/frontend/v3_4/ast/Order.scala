/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckable, SemanticExpressionCheck}
import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LogicalVariable}

case class OrderBy(sortItems: Seq[SortItem])(val position: InputPosition) extends ASTNode with SemanticCheckable {
  def semanticCheck = sortItems.semanticCheck

  def dependencies: Set[LogicalVariable] =
    sortItems.foldLeft(Set.empty[LogicalVariable]) { case (acc, item) => acc ++ item.expression.dependencies }
}

sealed trait SortItem extends ASTNode with SemanticCheckable {
  def expression: Expression
  def semanticCheck = SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression)

  def mapExpression(f: Expression => Expression): SortItem
}

case class AscSortItem(expression: Expression)(val position: InputPosition) extends SortItem {
  override def mapExpression(f: Expression => Expression) = copy(expression = f(expression))(position)
}

case class DescSortItem(expression: Expression)(val position: InputPosition) extends SortItem {
  override def mapExpression(f: Expression => Expression) = copy(expression = f(expression))(position)
}

