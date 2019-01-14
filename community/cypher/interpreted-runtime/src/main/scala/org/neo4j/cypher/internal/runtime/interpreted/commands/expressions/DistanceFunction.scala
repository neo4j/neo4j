/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

case class DistanceFunction(p1: Expression, p2: Expression) extends Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    (p1(ctx, state), p2(ctx, state)) match {
      case (geometry1: PointValue, geometry2: PointValue) => calculateDistance(geometry1, geometry2)
      case _ => Values.NO_VALUE
    }
  }

  def calculateDistance(geometry1: PointValue, geometry2: PointValue): AnyValue = {
    if (geometry1.getCoordinateReferenceSystem.equals(geometry2.getCoordinateReferenceSystem)) {
      Values.doubleValue(geometry1.getCoordinateReferenceSystem.getCalculator.distance(geometry1, geometry2))
    } else {
      Values.NO_VALUE
    }
  }

  override def rewrite(f: (Expression) => Expression) = f(DistanceFunction(p1.rewrite(f), p2.rewrite(f)))

  override def arguments: Seq[Expression] = p1.arguments ++ p2.arguments

  override def symbolTableDependencies: Set[String] = p1.symbolTableDependencies ++ p2.symbolTableDependencies

  override def toString: String = "Distance(" + p1 + ", " + p2 + ")"
}
