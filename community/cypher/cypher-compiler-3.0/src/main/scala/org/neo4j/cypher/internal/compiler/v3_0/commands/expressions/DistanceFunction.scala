/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import Math._

case class DistanceFunction(p1: Expression, p2: Expression) extends Expression {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    (p1(ctx), p2(ctx)) match {
      case (null, _) => null
      case (_, null) => null
      case (geometry1: Geometry, geometry2: Geometry) =>
        Seq(
          HaversinCalculator,
          CartesianCalculator
        ).collectFirst {
          case distance: DistanceCalculator if distance.isDefinedAt(geometry1, geometry2) =>
            distance(geometry1, geometry2)
        }.getOrElse(
            throw new IllegalArgumentException(s"Invalid points passed to distance($p1, $p2)")
          ).get
      case (x, y) => throw new CypherTypeException(s"Expected two Points, but got $x and $y")
    }
  }

  override def rewrite(f: (Expression) => Expression) = f(DistanceFunction(p1.rewrite(f), p2.rewrite(f)))

  override def calculateType(symbols: SymbolTable) = CTFloat

  override def arguments: Seq[Expression] = p1.arguments ++ p2.arguments

  override def symbolTableDependencies = p1.symbolTableDependencies ++ p2.symbolTableDependencies

  override def toString = "Distance(" + p1 + ", " + p2 + ")"
}

trait DistanceCalculator {
  def isDefinedAt(p1: Geometry, p2: Geometry): Boolean

  def calculateDistance(p1: Geometry, p2: Geometry): Double

  def apply(p1: Geometry, p2: Geometry): Option[Double] =
    if (isDefinedAt(p1, p2))
      Some(calculateDistance(p1, p2))
    else
      None
}

object CartesianCalculator extends DistanceCalculator {
  override def isDefinedAt(p1: Geometry, p2: Geometry) =
    p1.crs == CRS.Cartesian && p2.crs == CRS.Cartesian

  override def calculateDistance(p1: Geometry, p2: Geometry): Double = {
    val d2: Seq[Double] = (p1.coordinates zip p2.coordinates).map(x => pow(x._2 - x._1, 2))
    val sum = d2.foldLeft(0.0)((a, v) => a + v)
    sqrt(sum)
  }
}

object HaversinCalculator extends DistanceCalculator {

  private val EARTH_RADIUS_METERS = 6378140.0

  override def isDefinedAt(p1: Geometry, p2: Geometry): Boolean =
    p1.crs == CRS.WGS84 && p2.crs == CRS.WGS84

  override def calculateDistance(p1: Geometry, p2: Geometry): Double = {
    val c1: Seq[Double] = p1.coordinates.map(toRadians)
    val c2: Seq[Double] = p2.coordinates.map(toRadians)
    val dx = c2(0) - c1(0)
    val dy = c2(1) - c1(1)
    val a = pow(sin(dy / 2), 2.0) + cos(c1(1)) * cos(c2(1)) * pow(sin(dx / 2.0), 2.0)
    val greatCircleDistance = 2.0 * atan2(sqrt(a), sqrt(1-a))
    EARTH_RADIUS_METERS * greatCircleDistance
  }
}