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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}

/**
  * Like LetSemiApply, but with a precondition 'expr'. If 'expr' is true, 'idName' will to set to true without
  * executing right.
  *
  * for ( leftRow <- left ) {
  *   if ( leftRow.evaluate( expr) ) {
  *     produce leftRow
  *   } else {
  *     right.setArgument( leftRow )
  *     if ( right.nonEmpty ) {
  *       produce leftRow
  *     }
  *   }
  * }
  */
case class LetSelectOrSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression)
                               (val solved: PlannerQuery with CardinalityEstimation)
  extends AbstractLetSelectOrSemiApply(left, right, idName, expr, solved)

/**
  * Like LetAntiSemiApply, but with a precondition 'expr'. If 'expr' is true, 'idName' will to set to true without
  * executing right.
  *
  * for ( leftRow <- left ) {
  *   if ( leftRow.evaluate( expr) ) {
  *     produce leftRow
  *   } else {
  *     right.setArgument( leftRow )
  *     if ( right.isEmpty ) {
  *       produce leftRow
  *     }
  *   }
  * }
  */
case class LetSelectOrAntiSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression)
                                   (val solved: PlannerQuery with CardinalityEstimation)
  extends AbstractLetSelectOrSemiApply(left, right, idName, expr, solved)

abstract class AbstractLetSelectOrSemiApply(left: LogicalPlan, right: LogicalPlan, idName: IdName, expr: Expression,
                                            solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan {
  val lhs = Some(left)
  val rhs = Some(right)

  def availableSymbols: Set[IdName] = left.availableSymbols + idName
}
