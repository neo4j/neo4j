/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, RelTypeName, Variable}
import org.neo4j.cypher.internal.ir.v3_2.{CardinalityEstimation, IdName, PlannerQuery, VarPatternLength}

case class Expand(left: LogicalPlan,
                  from: IdName,
                  dir: SemanticDirection,
                  types: Seq[RelTypeName],
                  to: IdName, relName: IdName,
                  mode: ExpansionMode = ExpandAll)(val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(left)
  override def rhs = None
  override def availableSymbols: Set[IdName] = left.availableSymbols + relName + to
}

case class OptionalExpand(left: LogicalPlan, from: IdName, dir: SemanticDirection, types: Seq[RelTypeName], to: IdName, relName: IdName, mode: ExpansionMode = ExpandAll, predicates: Seq[Expression] = Seq.empty)
                         (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {
  override val lhs = Some(left)
  override def rhs = None
  override def availableSymbols = left.availableSymbols + relName + to
}

case class VarExpand(left: LogicalPlan,
                     from: IdName,
                     dir: SemanticDirection,
                     projectedDir: SemanticDirection,
                     types: Seq[RelTypeName],
                     to: IdName,
                     relName: IdName,
                     length: VarPatternLength,
                     mode: ExpansionMode = ExpandAll,
                     predicates: Seq[(Variable, Expression)] = Seq.empty)
                    (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(left)
  override def rhs = None

  override def availableSymbols = left.availableSymbols + relName + to
}

case class PruningVarExpand(left: LogicalPlan,
                            from: IdName,
                            dir: SemanticDirection,
                            types: Seq[RelTypeName],
                            to: IdName,
                            minLength: Int,
                            maxLength: Int,
                            predicates: Seq[(Variable, Expression)] = Seq.empty)
                           (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(left)
  override def rhs = None

  override def availableSymbols = left.availableSymbols + to
}

case class FullPruningVarExpand(left: LogicalPlan,
                                from: IdName,
                                dir: SemanticDirection,
                                types: Seq[RelTypeName],
                                to: IdName,
                                minLength: Int,
                                maxLength: Int,
                                predicates: Seq[(Variable, Expression)] = Seq.empty)
                               (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {

  override val lhs = Some(left)
  override def rhs = None

  override def availableSymbols = left.availableSymbols + to
}

sealed trait ExpansionMode
case object ExpandAll extends ExpansionMode
case object ExpandInto extends ExpansionMode
