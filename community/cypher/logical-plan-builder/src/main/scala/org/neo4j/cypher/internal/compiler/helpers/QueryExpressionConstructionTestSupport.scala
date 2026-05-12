/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.ToExpression
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.EntityFilterQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.MatchEntitySetQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList

trait QueryExpressionConstructionTestSupport {

  def between(
    gt: RangeGreaterThan[Expression],
    lt: RangeLessThan[Expression]
  ): RangeQueryExpression[InequalitySeekRangeWrapper] = {
    rangeExpression(
      RangeBetween(
        gt,
        lt
      )
    )
  }

  def rangeExpression(e: InequalitySeekRange[Expression]): RangeQueryExpression[InequalitySeekRangeWrapper] = {
    RangeQueryExpression(
      InequalitySeekRangeWrapper(e)(InputPosition.NONE)
    )
  }
  def composite(es: QueryExpression[Expression]*): CompositeQueryExpression[Expression] = CompositeQueryExpression(es)
  def single(e: ToExpression): SingleQueryExpression[Expression] = SingleQueryExpression(toExpression(e))
  def many(list: ToExpression): ManyQueryExpression[Expression] = ManyQueryExpression(toExpression(list))

  def gt(e: ToExpression): RangeGreaterThan[Expression] =
    RangeGreaterThan(NonEmptyList(ExclusiveBound(toExpression(e))))

  def gte(e: ToExpression): RangeGreaterThan[Expression] =
    RangeGreaterThan(NonEmptyList(InclusiveBound(toExpression(e))))
  def lt(e: ToExpression): RangeLessThan[Expression] = RangeLessThan(NonEmptyList(ExclusiveBound(toExpression(e))))
  def lte(e: ToExpression): RangeLessThan[Expression] = RangeLessThan(NonEmptyList(InclusiveBound(toExpression(e))))

  def matchEntities(e: ToExpression): MatchEntitySetQueryExpression[Expression] =
    MatchEntitySetQueryExpression(toExpression(e))

  def matchAll(): EntityFilterQueryExpression[Expression] = MatchAllQueryExpression

  private def toExpression(expr: ToExpression): Expression =
    expr match {
      case ToExpression.FromExpression(expr) => expr
      case ToExpression.FromString(_)        => throw new IllegalArgumentException("Strings are not supported yet")
    }
}

object QueryExpressionConstructionTestSupport extends QueryExpressionConstructionTestSupport
