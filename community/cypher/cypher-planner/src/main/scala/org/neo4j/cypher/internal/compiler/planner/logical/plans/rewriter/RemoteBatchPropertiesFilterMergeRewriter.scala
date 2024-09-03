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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter will
 */
object RemoteBatchPropertiesFilterMergeRewriter extends Rewriter with TopDownMergeableRewriter {

  private def isNonValidType(expression: Expression): Boolean =
    expression match {
      case Contains(lhs, rhs)           => isNonValidType(lhs) && isNonValidType(rhs)
      case EndsWith(lhs, rhs)           => isNonValidType(lhs) && isNonValidType(rhs)
      case StartsWith(lhs, rhs)         => isNonValidType(lhs) && isNonValidType(rhs)
      case LessThan(lhs, rhs)           => isNonValidType(lhs) && isNonValidType(rhs)
      case LessThanOrEqual(lhs, rhs)    => isNonValidType(lhs) && isNonValidType(rhs)
      case GreaterThanOrEqual(lhs, rhs) => isNonValidType(lhs) && isNonValidType(rhs)
      case GreaterThan(lhs, rhs)        => isNonValidType(lhs) && isNonValidType(rhs)
      case Equals(lhs, rhs)             => isNonValidType(lhs) && isNonValidType(rhs)
      case NotEquals(lhs, rhs)          => isNonValidType(lhs) && isNonValidType(rhs)
      case IsNull(expr)                 => isNonValidType(expr)
      case IsNotNull(expr)              => isNonValidType(expr)
      case Property(expr, _)            => isNonValidType(expr)
      case _: Parameter                 => false
      case _: Variable                  => false
      case _: Literal                   => false
      case _                            => true
    }

  private def isRewritableExpression(expression: Expression, availableSymbols: Set[LogicalVariable]): Boolean =
    expression.dependencies.size == 1 &&
      expression.dependencies.subsetOf(availableSymbols) &&
      expression.subExpressions.exists(_.isInstanceOf[Property]) &&
      !expression.subExpressions.exists(isNonValidType)

  override val innerRewriter: Rewriter = {
    Rewriter.lift {
      case filter @ Selection(
          predicate,
          remoteBatchProperties: RemoteBatchProperties
        ) =>
        val (inlinablePreds, nonInlinablePreds) = predicate.exprs
          .partition(isRewritableExpression(_, remoteBatchProperties.properties.flatMap(_.dependencies)))
        if (inlinablePreds.nonEmpty) {
          val newPlan = RemoteBatchPropertiesWithFilter(
            remoteBatchProperties.source,
            inlinablePreds,
            remoteBatchProperties.properties
          )(SameId(remoteBatchProperties.id))
          if (nonInlinablePreds.nonEmpty)
            Selection(nonInlinablePreds.toSeq, newPlan)(SameId(filter.id))
          else
            newPlan
        } else
          filter
    }
  }

  private val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
