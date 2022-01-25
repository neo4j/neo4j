/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedAmbiguousGroupingNotification
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case class OrderBy(sortItems: Seq[SortItem])(val position: InputPosition) extends ASTNode with SemanticCheckable {
  def semanticCheck: SemanticCheck = sortItems.semanticCheck

  def checkAmbiguousOrdering(returnItems: ReturnItems, nameOfClause: String): SemanticCheck = (state: SemanticState) => {
    val (aggregationItems, groupingItems) = returnItems.items.partition(item => item.expression.containsAggregate)
    val groupingVariablesAndAliases = groupingItems.map(_.expression).collect { case v: LogicalVariable => v } ++ returnItems.items.flatMap(_.alias)
    val propertiesUsedForGrouping = groupingItems.map(_.expression).collect { case v@LogicalProperty(LogicalVariable(_), _) => v }

    val stateWithNotifications = if (aggregationItems.nonEmpty) {
      val ambiguousSortItems = sortItems.filter(
        sortItem => AmbiguousAggregation.isDeprecatedExpression(sortItem.originalExpression, groupingVariablesAndAliases, propertiesUsedForGrouping))
      if (ambiguousSortItems.nonEmpty) {
        state.addNotification(
          DeprecatedAmbiguousGroupingNotification(
            sortItems.head.position,
            Order.getAmbiguousNotificationDetails(sortItems, groupingItems, returnItems.items, nameOfClause)
          ))
      } else {
        state
      }
    } else {
      state
    }

    SemanticCheckResult.success(stateWithNotifications)
  }

  def dependencies: Set[LogicalVariable] =
    sortItems.foldLeft(Set.empty[LogicalVariable]) { case (acc, item) => acc ++ item.expression.dependencies }
}

object Order {
  private val ExprStringifier = ExpressionStringifier(e => e.asCanonicalStringVal)

  /**
   * If possible, creates a notification detail which describes how this query can be rewritten to use an extra `WITH` clause.
   *
   * Example:
   * RETURN n.x + n.y, count(*) ORDER BY n.x + n.y, where n is a variable
   * ->
   * WITH n.x + n.y AS grpExpr0 RETURN grpExpr0, count(*) ORDER BY grpExpr0
   *
   * @param sortItems        sortItems
   * @param allGroupingItems grouping items
   * @param allReturnItems   both grouping items and items which expression contains an aggregation.
   * @return
   */
  def getAmbiguousNotificationDetails(sortItems: Seq[SortItem],
                                      allGroupingItems: Seq[ReturnItem],
                                      allReturnItems: Seq[ReturnItem],
                                      nameOfClause: String): Option[String] = {
    val deprecatedGroupingKeys = AmbiguousAggregation.deprecatedGroupingKeysUsedInAggrExpr(sortItems.map(_.originalExpression), allGroupingItems)
    if (deprecatedGroupingKeys.nonEmpty) {
      val (withItems, returnItems) = AmbiguousAggregation.getAliasedWithAndReturnItems(allReturnItems)
      val aliasedWithItems = withItems.collect { case item: AliasedReturnItem => item }
      val rewrittenOrderBy = replaceExpressionWithAlias(sortItems, aliasedWithItems)

      if (!AmbiguousAggregation.containsDeprecatedAggrExpr(rewrittenOrderBy.map(_.expression), returnItems)) {
        val singleDeprecatedGK = deprecatedGroupingKeys.size == 1
        Some(s"The grouping key${if (singleDeprecatedGK) "" else "s"} " +
          s"${deprecatedGroupingKeys.map(_.expression).map(ExprStringifier(_)).mkString("`", "`, `", "`")} " +
          s"${if (singleDeprecatedGK) "is" else "are"} deprecated. Could be rewritten using a `WITH`: " +
          s"`WITH ${withItems.map(_.stringify(ExprStringifier)).mkString(", ")}" +
          s" $nameOfClause ${returnItems.map(_.stringify(ExprStringifier)).mkString(", ")}" +
          s" ORDER BY ${rewrittenOrderBy.map(_.stringify(ExprStringifier)).mkString(", ")}`")
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Replace all grouping expressions in `sortItems` with the matching alias from `groupingExprs`
   *
   * @param sortItems     sort items
   * @param groupingExprs grouping items
   * @return the expression
   */
  private def replaceExpressionWithAlias(sortItems: Seq[SortItem], groupingExprs: Seq[AliasedReturnItem]): Seq[SortItem] = {
    val rewriter = topDown(Rewriter.lift {
      case expr: Expression =>
        val matchingGroupingExpr = groupingExprs.find(returnItem => expr == returnItem.expression)

        if (matchingGroupingExpr.nonEmpty) {
          Variable(matchingGroupingExpr.get.name)(InputPosition.NONE)
        } else {
          expr
        }
    })

    sortItems.map { sortItem =>
      val rewrittenExpression = rewriter.apply(sortItem.originalExpression).asInstanceOf[Expression]
      sortItem match {
        case ascSortItem: AscSortItem => AscSortItem(rewrittenExpression)(ascSortItem.position, ascSortItem.originalExpression)
        case descSortItem: DescSortItem => DescSortItem(rewrittenExpression)(descSortItem.position, descSortItem.originalExpression)
      }
    }
  }
}

sealed trait SortItem extends ASTNode with SemanticCheckable {
  def expression: Expression
  def originalExpression: Expression
  def semanticCheck: SemanticCheck = SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression) chain
    SemanticPatternCheck.checkValidPropertyKeyNames(expression.findAllByClass[Property].map(prop => prop.propertyKey), expression.position)
  def stringify(expressionStringifier: ExpressionStringifier): String
  def mapExpression(f: Expression => Expression): SortItem
}

case class AscSortItem(expression: Expression)(val position: InputPosition, val originalExpression: Expression = expression) extends SortItem {
  override def mapExpression(f: Expression => Expression): AscSortItem = copy(expression = f(expression))(position, originalExpression)

  override def dup(children: Seq[AnyRef]): AscSortItem.this.type =
    AscSortItem(children.head.asInstanceOf[Expression])(position, originalExpression).asInstanceOf[this.type]

  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} ASC"

  override def stringify(expressionStringifier: ExpressionStringifier): String = s"${expressionStringifier(expression)} ASC"
}

case class DescSortItem(expression: Expression)(val position: InputPosition, val originalExpression: Expression = expression) extends SortItem {
  override def mapExpression(f: Expression => Expression): DescSortItem = copy(expression = f(expression))(position, originalExpression)

  override def dup(children: Seq[AnyRef]): DescSortItem.this.type =
    DescSortItem(children.head.asInstanceOf[Expression])(position, originalExpression).asInstanceOf[this.type]
  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} DESC"

  override def stringify(expressionStringifier: ExpressionStringifier): String = s"${expressionStringifier(expression)} DESC"
}