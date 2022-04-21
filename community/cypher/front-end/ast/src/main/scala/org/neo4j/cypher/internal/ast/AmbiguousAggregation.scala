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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

object AmbiguousAggregation {

  /**
   * ReturnItem will be unaliased if:
   * - it is auto-aliased and the expression is a variable (only variables can be unaliased in a `WITH`-clause)
   *
   * ReturnItems will get a new alias if:
   * - It is auto-aliased
   * - It is unaliased and NOT a variable
   * @param returnItems original return items
   * @return
   */
  private def aliasItems(returnItems: Seq[ReturnItem]): Seq[ReturnItem] = {
    var i = -1
    def newGroupingAlias(returnItem: ReturnItem): ReturnItem = {
      i += 1
      AliasedReturnItem(returnItem.expression, Variable(s"grpExpr$i")(InputPosition.NONE))(
        InputPosition.NONE,
        isAutoAliased = true
      )
    }

    returnItems.map {
      case aliasedReturnItem @ AliasedReturnItem(expr, _)
        if aliasedReturnItem.isAutoAliased && (expr.containsAggregate || expr.isInstanceOf[LogicalVariable]) =>
        UnaliasedReturnItem(aliasedReturnItem.expression, "")(InputPosition.NONE)
      case aliasedReturnItem: AliasedReturnItem if aliasedReturnItem.isAutoAliased =>
        newGroupingAlias(aliasedReturnItem)
      case returnItem: UnaliasedReturnItem if !returnItem.expression.isInstanceOf[LogicalVariable] =>
        newGroupingAlias(returnItem)
      case returnItem => returnItem
    }
  }

  /**
   * Replace all grouping expressions in `expression` with the matching alias from `groupingExprs`
   *
   * @param expression    the origina expression
   * @param groupingExprs aliased grouping expressions
   * @return the expression
   */
  private def replaceExpressionWithAlias(expression: ReturnItem, groupingExprs: Seq[AliasedReturnItem]): ReturnItem = {
    val rewriter = topDown(Rewriter.lift {
      case expr: Expression =>
        groupingExprs
          .find(groupingExpr => expr == groupingExpr.expression)
          .map(_.variable)
          .getOrElse(expr)
    })

    expression.endoRewrite(rewriter)
  }

  /**
   * In an expression that contains aggregation or in a sort expressions, the leaves of expressions must be one of:
   * - An aggregation
   * - A literal
   * - A Parameter
   * - A variable - ONLY IF that variable is also a projection expression on its own (e.g. the n in RETURN n AS myNode, n.value + count(*))
   * - Property access - ONLY IF that property access is also a projection expression on its own (e.g. the n.prop in RETURN n.prop, n.prop + count(*))
   * - Non-nested map access - ONLY IF that map access is also a projection expression on its own (e.g. the map.prop in WITH {prop: 2} AS map RETURN map.prop, map.prop + count(*))
   *
   * @param sortOrAggregationExpr              the aggregation expression to check
   * @param variablesUsedForGrouping           all variable grouping items
   * @param nonNestedPropertiesUsedForGrouping all non-nested property grouping items
   * @return true if the given aggregation expression does not follow the rules above
   */
  def isDeprecatedExpression(
    sortOrAggregationExpr: Expression,
    variablesUsedForGrouping: Seq[LogicalVariable],
    nonNestedPropertiesUsedForGrouping: Seq[LogicalProperty]
  ): Boolean =
    sortOrAggregationExpr.folder.treeFold(Expression.TreeAcc[Boolean](false)) {
      case scope: ScopeExpression =>
        acc =>
          val newAcc = acc.pushScope(scope.introducedVariables)
          TraverseChildrenNewAccForSiblings(newAcc, _.popScope)
      case IsAggregate(_) =>
        acc => SkipChildren(acc)
      case e: LogicalVariable if !variablesUsedForGrouping.contains(e) =>
        acc => if (!acc.inScope(e)) SkipChildren(acc.mapData(_ => true)) else SkipChildren(acc)
      case e: LogicalProperty if nonNestedPropertiesUsedForGrouping.contains(e) =>
        acc => SkipChildren(acc)
    }.data

  /**
   * Returns all deprecated grouping keys used in the given expressions.
   *
   * A grouping key is deprecated if it is an expression which is also used in the aggregation expression or order by and
   * the leaves of expressions is not one of:
   * - An aggregation
   * - A literal
   * - A Parameter
   * - A variable - ONLY IF that variable is also a projection expression on its own (e.g. the n in RETURN n AS myNode, n.value + count(*))
   * - Property access - ONLY IF that property access is also a projection expression on its own (e.g. the n.prop in RETURN n.prop, n.prop + count(*))
   * - Non-nested map access - ONLY IF that map access is also a projection expression on its own (e.g. the map.prop in WITH {prop: 2} AS map RETURN map.prop, map.prop + count(*))
   *
   * @param expression    the  expressions (should either be an aggregation expression or sort expression)
   * @param groupingItems all grouping items
   * @return the deprecated grouping keys used in the expressions
   */
  def deprecatedGroupingKeysUsedInAggrExpr(
    expression: Seq[Expression],
    groupingItems: Seq[ReturnItem]
  ): Seq[ReturnItem] = {
    expression.folder.treeFold(Seq.empty[ReturnItem]) {
      case IsAggregate(_) | _: LogicalVariable | LogicalProperty(LogicalVariable(_), _) =>
        acc => SkipChildren(acc)
      case e: Expression =>
        groupingItems
          .find(_.expression == e)
          // An expression which only contains literals/parameters is not deprecated -> must contain a variable
          .filter(_.expression.folder.treeExists { case _: LogicalVariable => true })
          .map(deprecatedGroupingKey => (acc: Seq[ReturnItem]) => SkipChildren(acc :+ deprecatedGroupingKey))
          .getOrElse(acc => TraverseChildren(acc))
    }
  }

  def getAliasedWithAndReturnItems(allReturnItems: Seq[ReturnItem]): (Seq[ReturnItem], Seq[ReturnItem]) = {
    val newReturnItems = AmbiguousAggregation.aliasItems(allReturnItems)
    val withItems = newReturnItems.filterNot(_.expression.containsAggregate)
    val aliasedWithItems = withItems.collect { case item: AliasedReturnItem => item }
    val returnItems = newReturnItems.map {
      // Update aggregation expression to use variables projected in preceding with-clause
      case returnItem if returnItem.expression.containsAggregate =>
        AmbiguousAggregation.replaceExpressionWithAlias(returnItem, aliasedWithItems)
      // AliasedReturnItem has been projected in preceding `WITH`-clause.
      case returnItem: AliasedReturnItem   => UnaliasedReturnItem(returnItem.variable, "")(InputPosition.NONE)
      case returnItem: UnaliasedReturnItem => returnItem
    }

    (withItems, returnItems)
  }

  /**
   * Checks if any return item contains an deprecated aggregation expression
   *
   * @param returnItems all return items
   * @return true if aggregation expression contains deprecated aggregation expression
   */
  def containsDeprecatedAggrExpr(expressionsToCheck: Seq[Expression], returnItems: Seq[ReturnItem]): Boolean = {
    val expressions = returnItems.map(_.expression)
    val newGroupingVariables = expressions.collect { case expr: LogicalVariable => expr }
    val newPropertiesUsedForGrouping = expressions.collect { case v @ LogicalProperty(LogicalVariable(_), _) => v }

    expressionsToCheck.exists(aggrExpr =>
      AmbiguousAggregation.isDeprecatedExpression(aggrExpr, newGroupingVariables, newPropertiesUsedForGrouping)
    )
  }
}
