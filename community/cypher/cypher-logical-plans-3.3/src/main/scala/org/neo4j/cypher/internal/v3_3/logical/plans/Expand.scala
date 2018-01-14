/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_3.logical.plans

import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, RelTypeName, Variable}
import org.neo4j.cypher.internal.ir.v3_3.{CardinalityEstimation, PlannerQuery, VarPatternLength}

case class Expand(left: LogicalPlan,
                  from: String,
                  dir: SemanticDirection,
                  types: Seq[RelTypeName],
                  to: String, relName: String,
                  mode: ExpansionMode = ExpandAll)(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(left)
  override def rhs = None
  override val availableSymbols: Set[String] = left.availableSymbols + relName + to
}

case class OptionalExpand(left: LogicalPlan, from: String, dir: SemanticDirection, types: Seq[RelTypeName], to: String, relName: String, mode: ExpansionMode = ExpandAll, predicates: Seq[Expression] = Seq.empty)
                         (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(left)
  override def rhs = None
  override val availableSymbols = left.availableSymbols + relName + to
}

case class VarExpand(left: LogicalPlan,
                     from: String,
                     dir: SemanticDirection,
                     projectedDir: SemanticDirection,
                     types: Seq[RelTypeName],
                     to: String,
                     relName: String,
                     length: VarPatternLength,
                     mode: ExpansionMode = ExpandAll,
                     tempNode: String,
                     tempEdge: String,
                     nodePredicate: Expression,
                     edgePredicate: Expression,
                     legacyPredicates: Seq[(Variable, Expression)])
                    (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(left)
  override def rhs = None

  override val availableSymbols: Set[String] = left.availableSymbols + relName + to
}

case class PruningVarExpand(left: LogicalPlan,
                            from: String,
                            dir: SemanticDirection,
                            types: Seq[RelTypeName],
                            to: String,
                            minLength: Int,
                            maxLength: Int,
                            predicates: Seq[(Variable, Expression)] = Seq.empty)
                           (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(left)
  override def rhs = None

  override val availableSymbols: Set[String] = left.availableSymbols + to
}

sealed trait ExpansionMode
case object ExpandAll extends ExpansionMode
case object ExpandInto extends ExpansionMode
