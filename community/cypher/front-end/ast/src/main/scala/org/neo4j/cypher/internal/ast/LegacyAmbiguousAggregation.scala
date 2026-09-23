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

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren

// This object contains legacy logic only relevant for Cypher 5.
// This will be removed together with Cypher 5 in the future.
// Do not use.
object LegacyAmbiguousAggregation {

  /**
   * An aggregation expression in a RETURN/WITH clause structurally equals one in that same clause's
   * ORDER BY/WHERE only if the variables it depends on still mean the same thing in both places. This is
   * not the case when some other item of the same clause redefines (shadows) one of those variables with
   * a new alias, since ORDER BY/WHERE resolve names in the post-projection scope: e.g. in
   * `RETURN 1 AS a, count(a) AS cnt ORDER BY count(a) + 1`, the `a` inside the ORDER BY's `count(a)` binds
   * to the new `1 AS a`, not to the pre-projection `a` the RETURN's own `count(a)` aggregates over, even
   * though the two `count(a)` expressions are structurally identical (equality here ignores position).
   */
  def potentiallyRedefined(
    expression: Expression,
    aliasMap: Map[Expression, LogicalVariable]
  ): Boolean =
    aliasMap.filter {
      case (e: Variable, v: Variable) => e.name != v.name
      case _                          => true
    }.valuesIterator.exists(alias => expression.folder.findAllByClass[LogicalVariable].contains(alias))

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

  def checkIllegalOrdering(orderBy: OrderBy, returnItems: ReturnItems): Option[SemanticError] = {
    val rawAggregationItems = returnItems.items
      .filter(item => item.expression.containsAggregate)
      .map(_.expression)

    if (rawAggregationItems.nonEmpty) {
      // An aggregation item only counts as "already projected" for a sort item that references it under
      // the same variable bindings. If some other item of this clause redefines a variable the aggregation
      // depends on, the match is coincidental (see potentiallyRedefined), and the sort item's use of that
      // aggregation must be treated as not projected, not silently accepted.
      val aliasMap: Map[Expression, LogicalVariable] = returnItems.items.collect {
        case AliasedReturnItem(expression, variable) => expression -> variable
      }.toMap
      val aggregationItems = rawAggregationItems.filterNot(potentiallyRedefined(_, aliasMap)).toSet

      val illegalSortItems =
        orderBy.sortItems.flatMap(sortItem => notProjectedAggregationExpression(sortItem.expression, aggregationItems))

      if (illegalSortItems.nonEmpty) {
        val sortItem = illegalSortItems.head
        val prettifier = ExpressionStringifier()
        Some(SemanticError.aggregateExpressionsInOrderBy(
          illegalSortItems.map(e => prettifier.apply(e)),
          sortItem.position
        ))
      } else {
        None
      }
    } else {
      None
    }
  }

}
