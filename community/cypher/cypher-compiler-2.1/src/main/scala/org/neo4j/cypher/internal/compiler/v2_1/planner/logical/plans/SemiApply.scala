/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_1.planner.{Exists, Selections}

case class SemiApply(outer: LogicalPlan, inner: LogicalPlan)(subQuery: Exists) extends AbstractSemiApply(outer, inner, subQuery)
case class AntiSemiApply(outer: LogicalPlan, inner: LogicalPlan)(subQuery: Exists) extends AbstractSemiApply(outer, inner, subQuery)

abstract class AbstractSemiApply(outer: LogicalPlan, inner: LogicalPlan, val subQuery: Exists) extends LogicalPlan {
  val lhs = Some(outer)
  val rhs = Some(inner)
}

object AbstractSemiApply {
  def solved(outer: QueryPlan, inner: QueryPlan, subQuery: Exists) = {
    val newSelections = Selections(outer.solved.selections.predicates + subQuery.predicate)
    outer.solved.copy(
      subQueries = outer.solved.subQueries :+ subQuery,
      selections = newSelections,
      argumentIds = subQuery.queryGraph.argumentIds
    )
  }
}

object SemiApplyPlan {
  def apply(outer: QueryPlan, inner: QueryPlan, subQuery: Exists) =
    QueryPlan(
      SemiApply(outer.plan, inner.plan)(subQuery),
      AbstractSemiApply.solved(outer, inner, subQuery)
    )
}

object AntiSemiApplyPlan {
  def apply(outer: QueryPlan, inner: QueryPlan, subQuery: Exists) =
    QueryPlan(
      AntiSemiApply(outer.plan, inner.plan)(subQuery),
      AbstractSemiApply.solved(outer, inner, subQuery)
    )
}
