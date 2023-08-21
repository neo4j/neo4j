/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.AmbiguousAggregation.notProjectedAggregationExpression
import org.neo4j.cypher.internal.ast.Order.notProjectedAggregations
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

case class OrderBy(sortItems: Seq[SortItem])(val position: InputPosition) extends ASTNode with SemanticCheckable {
  def semanticCheck: SemanticCheck = sortItems.semanticCheck

  def checkIllegalOrdering(returnItems: ReturnItems): Option[SemanticError] = {
    val aggregationItems = returnItems.items
      .filter(item => item.expression.containsAggregate)
      .map(_.expression)
      .toSet

    if (aggregationItems.nonEmpty) {
      val illegalSortItems =
        sortItems.flatMap(sortItem => notProjectedAggregationExpression(sortItem.expression, aggregationItems))

      if (illegalSortItems.nonEmpty) {
        Some(SemanticError(
          notProjectedAggregations(illegalSortItems.map(_.asCanonicalStringVal)),
          illegalSortItems.head.position
        ))
      } else {
        None
      }
    } else {
      None
    }
  }

  def dependencies: Set[LogicalVariable] =
    sortItems.foldLeft(Set.empty[LogicalVariable]) { case (acc, item) => acc ++ item.expression.dependencies }
}

object Order {

  def notProjectedAggregations(variables: Seq[String]): String =
    s"Illegal aggregation expression(s) in order by: ${variables.mkString(", ")}. " +
      "If an aggregation expression is used in order by, it also needs to be a projection item on it's own. " +
      "For example, in 'RETURN n.a, 1 + count(*) ORDER BY count(*) + 1' the aggregation expression 'count(*) + 1' is not a projection " +
      "item on its own, but it could be rewritten to 'RETURN n.a, 1 + count(*) AS cnt ORDER BY 1 + count(*)'."
}

sealed trait SortItem extends ASTNode with SemanticCheckable {
  def expression: Expression

  def semanticCheck: SemanticCheck = SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression) chain
    SemanticPatternCheck.checkValidPropertyKeyNames(
      expression.folder.findAllByClass[Property].map(prop => prop.propertyKey)
    )
  def stringify(expressionStringifier: ExpressionStringifier): String
  def mapExpression(f: Expression => Expression): SortItem
}

case class AscSortItem(expression: Expression)(
  val position: InputPosition
) extends SortItem {

  override def mapExpression(f: Expression => Expression): AscSortItem =
    copy(expression = f(expression))(position)

  override def dup(children: Seq[AnyRef]): AscSortItem.this.type =
    AscSortItem(children.head.asInstanceOf[Expression])(position).asInstanceOf[this.type]

  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} ASC"

  override def stringify(expressionStringifier: ExpressionStringifier): String =
    s"${expressionStringifier(expression)} ASC"
}

case class DescSortItem(expression: Expression)(
  val position: InputPosition
) extends SortItem {

  override def mapExpression(f: Expression => Expression): DescSortItem =
    copy(expression = f(expression))(position)

  override def dup(children: Seq[AnyRef]): DescSortItem.this.type =
    DescSortItem(children.head.asInstanceOf[Expression])(position).asInstanceOf[this.type]
  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} DESC"

  override def stringify(expressionStringifier: ExpressionStringifier): String =
    s"${expressionStringifier(expression)} DESC"
}
