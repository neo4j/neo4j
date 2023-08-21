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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings

object AmbiguousAggregation {

  /**
   * In a RETURN/WITH expression that contains an aggregation or in a sort item where there is an aggregation in the preceding WITH/RETURN clause,
   * the leaves of the expression must be one of:
   * - An aggregation
   * - A literal
   * - A Parameter
   * - Element property access on a variable - ONLY IF that property access is also a projection expression on its own
   * - Non nested static map access on a variable - ONLY IF that map access is also a projection expression on its own
   * - A variable - ONLY IF that variable is also a projection expression on its own (e.g. the n in RETURN n AS myNode, n.value + count(*))
   *
   * @param sortOrAggregationExpr              the expression to check for ambiguous leaf expressions
   * @param variablesUsedForGrouping           the allowed variables (and aliases in case we verify a sort expression)
   * @param nonNestedPropertiesUsedForGrouping the allowed properties
   * @return the ambiguous expressions
   */
  def ambiguousExpressions(
    sortOrAggregationExpr: Expression,
    variablesUsedForGrouping: Set[LogicalVariable],
    nonNestedPropertiesUsedForGrouping: Set[LogicalProperty]
  ): Seq[Expression] =
    sortOrAggregationExpr.folder.treeFold(Expression.TreeAcc[Seq[Expression]](Seq.empty)) {
      case scope: ScopeExpression =>
        acc =>
          val newAcc = acc.pushScope(scope.introducedVariables)
          TraverseChildrenNewAccForSiblings(newAcc, _.popScope)
      case IsAggregate(_) =>
        acc => SkipChildren(acc)
      case e: LogicalVariable if !variablesUsedForGrouping.contains(e) =>
        acc => if (!acc.inScope(e)) SkipChildren(acc.mapData(exprs => exprs :+ e)) else SkipChildren(acc)
      case e: LogicalProperty if nonNestedPropertiesUsedForGrouping.contains(e) =>
        acc => SkipChildren(acc)
      case p @ LogicalProperty(v: LogicalVariable, _) =>
        acc => // In order to report, in the error message, a property access as invalid, we do a separate check for properties here.
          if (!variablesUsedForGrouping.contains(v) && !acc.inScope(v)) SkipChildren(acc.mapData(exprs => exprs :+ p))
          else SkipChildren(acc)
    }.data

  /**
   * It is not allowed to use aggregating expressions in the ORDER BY sub-clause if they are not also listed in the projecting clause.
   * This function makes sure all aggregation expressions in the order by clause can be replaced by the alias of the projection item
   * in the preceding WITH/RETURN clause.
   * Note: Only checks for exact matches of the projection clause, it therefore depends on how the expression tree is built up
   *
   * ex:
   * Valid:
   * MATCH (n) RETURN n, count(*)      AS cnt ORDER BY  count(*) + 1 + 2
   * MATCH (n) RETURN n, count(*) + 1  AS cnt ORDER BY  count(*) + 1 + 2
   * MATCH (n) RETURN n, count(*) + 1  AS cnt ORDER BY  2 + (count(*) + 1)
   *
   * Invalid:
   * MATCH (n) RETURN n, 1 + count(*)  AS cnt ORDER BY  count(*) + 1 + 2
   * MATCH (n) RETURN n, count(*) + 1  AS cnt ORDER BY  2 + count(*) + 1
   *
   * @param sortExpression the expression to verify
   * @param aggregationGroupingExpressions the allowed aggregation expressions in the sort expression
   * @return the aggregation expressions which are not projected
   */
  def notProjectedAggregationExpression(
    sortExpression: Expression,
    aggregationGroupingExpressions: Set[Expression]
  ): Seq[Expression] = {
    sortExpression.folder.treeFold(Expression.TreeAcc[Seq[Expression]](Seq.empty)) {
      case expr: Expression if aggregationGroupingExpressions.contains(expr) =>
        acc => SkipChildren(acc)
      case IsAggregate(expr: Expression) =>
        acc => SkipChildren(acc.mapData(exprs => exprs :+ expr))
    }.data
  }
}
