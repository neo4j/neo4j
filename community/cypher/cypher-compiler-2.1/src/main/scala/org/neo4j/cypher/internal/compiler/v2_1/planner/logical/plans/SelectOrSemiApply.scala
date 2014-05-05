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
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

case class SelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, predicate: Expression)(exists: Exists) extends AbstractSelectOrSemiApply(outer, inner, predicate, exists)
case class SelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, predicate: Expression)(exists: Exists) extends AbstractSelectOrSemiApply(outer, inner, predicate, exists)

abstract class AbstractSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, predicate: Expression, exists: Exists) extends LogicalPlan {
  val lhs = Some(outer)
  val rhs = Some(inner)
}

object AbstractSelectOrSemiApply {
  def solved(outer: QueryPlan, exists: Exists) = {
    val newSelections = Selections(outer.solved.selections.predicates + exists.predicate)
    outer.solved.copy(subQueries = outer.solved.subQueries :+ exists, selections = newSelections)
  }
}

object SelectOrSemiApplyPlan {
  def apply(outer: QueryPlan, inner: QueryPlan, predicate: Expression, exists: Exists) =
    QueryPlan(
      SelectOrSemiApply(outer.plan, inner.plan, predicate)(exists),
      AbstractSelectOrSemiApply.solved(outer, exists)
    )
}

object SelectOrAntiSemiApplyPlan {
  def apply(outer: QueryPlan, inner: QueryPlan, predicate: Expression, exists: Exists) =
    QueryPlan(
      SelectOrAntiSemiApply(outer.plan, inner.plan, predicate)(exists),
      AbstractSelectOrSemiApply.solved(outer, exists)
    )
}
