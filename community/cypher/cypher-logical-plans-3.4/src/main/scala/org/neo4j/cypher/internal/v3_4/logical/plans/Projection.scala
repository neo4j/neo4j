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
  * For each source row, produce the source row augmented with 'expressions'. For entry in
  * 'expressions', the produced row get an extra variable name as the key, with the value of
  * the expression.
  */
case class Projection(source: LogicalPlan, expressions: Map[String, Expression])
                     (val solved: PlannerQuery with CardinalityEstimation) extends LogicalPlan with LazyLogicalPlan {
  val lhs = Some(source)
  val rhs = None

  val availableSymbols: Set[IdName] = source.availableSymbols ++ expressions.keySet.map(IdName(_))
}
