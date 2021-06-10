/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertyScannable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsPropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsStringRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsValueRangeSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.PropertySeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.Seekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.MultipleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NonSeekablePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NotExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.SingleExactPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.variable
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialDistanceSeekWrapper
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols.CTAny

trait IndexCompatiblePredicatesProvider {

  /**
   * Collects predicates which could be used to justify the use of an index. Some may be collected from the predicates provided here. Others might be
   * implicitly inferred (e.g. through constraints, see implicitIndexCompatiblePredicates)
   *
   * @param predicates            selection predicates from the where clause
   * @param argumentIds           argument ids provided to this sub-plan
   * @param semanticTable         semantic table
   * @param planContext           planContext to ask for indexes
   * @param aggregatingProperties properties used in query aggregations, which are implicitly assumed to not be null
   */
  private[index] def findIndexCompatiblePredicates(
                                                    predicates: Set[Expression],
                                                    argumentIds: Set[String],
                                                    semanticTable: SemanticTable,
                                                    planContext: PlanContext,
                                                    aggregatingProperties: Set[PropertyAccess],
                                                  ): Set[IndexCompatiblePredicate] = {
    val arguments: Set[LogicalVariable] = argumentIds.map(variable)

    def valid(ident: LogicalVariable, dependencies: Set[LogicalVariable]): Boolean =
      !arguments.contains(ident) && dependencies.subsetOf(arguments)

    val explicitCompatiblePredicates = predicates.flatMap {
      // n.prop IN [ ... ]
      case predicate@AsPropertySeekable(seekable: PropertySeekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.args.asQueryExpression
        val exactness = if (queryExpression.isInstanceOf[SingleQueryExpression[_]]) SingleExactPredicate else MultipleExactPredicate
        Set(IndexCompatiblePredicate(seekable.ident, seekable.expr, predicate, queryExpression, seekable.propertyValueType(semanticTable),
          predicateExactness = exactness, solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // ... = n.prop
      // In some rare cases, we can't rewrite these predicates cleanly,
      // and so planning needs to search for these cases explicitly
      case predicate@Equals(a, prop@Property(variable: LogicalVariable, _)) if valid(variable, a.dependencies) =>
        val expr = SingleQueryExpression(a)
        Set(IndexCompatiblePredicate(variable, prop, predicate, expr, Seekable.cypherTypeForTypeSpec(semanticTable.getActualTypeFor(prop)),
          predicateExactness = SingleExactPredicate, solvedPredicate = Some(predicate), dependencies = a.dependencies))

      // n.prop STARTS WITH "prefix%..."
      case predicate@AsStringRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable),
          predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // n.prop <|<=|>|>= value
      case predicate@AsValueRangeSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable),
          predicateExactness = NotExactPredicate, solvedPredicate = Some(predicate), dependencies = seekable.dependencies))

      // An index seek for this will almost satisfy the predicate, but with the possibility of some false positives.
      // Since it reduces the cardinality to almost the level of the predicate, we can use the predicate to calculate cardinality,
      // but not mark it as solved, since the planner will still need to solve it with a Filter.
      case predicate@AsDistanceSeekable(seekable) if valid(seekable.ident, seekable.dependencies) =>
        val queryExpression = seekable.asQueryExpression
        Set(IndexCompatiblePredicate(seekable.ident, seekable.property, predicate, queryExpression, seekable.propertyValueType(semanticTable),
          predicateExactness = NotExactPredicate, solvedPredicate = Some(PartialDistanceSeekWrapper(predicate)), dependencies = seekable.dependencies))

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@AsPropertyScannable(scannable) if valid(scannable.ident, Set.empty) =>
        Set(IndexCompatiblePredicate(scannable.ident, scannable.property, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NotExactPredicate,
          solvedPredicate = Some(predicate), dependencies = Set.empty).convertToScannable)

      // n.prop ENDS WITH 'substring'
      case predicate@EndsWith(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      // n.prop CONTAINS 'substring'
      case predicate@Contains(prop@Property(variable: Variable, _), expr) if valid(variable, expr.dependencies) =>
        Set(IndexCompatiblePredicate(variable, prop, predicate, ExistenceQueryExpression(), CTAny, predicateExactness = NonSeekablePredicate,
          solvedPredicate = Some(predicate), dependencies = expr.dependencies))

      case _ =>
        Set.empty[IndexCompatiblePredicate]
    }

    val implicitCompatiblePredicates = implicitIndexCompatiblePredicates(planContext, aggregatingProperties, predicates, explicitCompatiblePredicates, valid)

    explicitCompatiblePredicates ++ implicitCompatiblePredicates
  }

  /**
   * Find any implicit index compatible predicates.
   *
   * @param planContext                  planContext to ask for indexes
   * @param aggregatingProperties        A set of all properties over which aggregation is performed,
   *                                     where we potentially could use an IndexScan.
   *                                     E.g. WITH n.prop1 AS prop RETURN min(prop), count(m.prop2) => Set(PropertyAccess("n", "prop1"), PropertyAccess("m", "prop2"))
   * @param predicates                   the predicates in the query
   * @param explicitCompatiblePredicates the explicit index compatible predicates that were extracted from predicates
   * @param valid                        a test that can be applied to check if an implicit predicate is valid
   *                                     based on its variable and dependencies as arguments to the lambda function.
   */
  protected def implicitIndexCompatiblePredicates(planContext: PlanContext,
                                                  aggregatingProperties: Set[PropertyAccess],
                                                  predicates: Set[Expression],
                                                  explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
                                                  valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[IndexCompatiblePredicate]
}
