/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._


case class Distance(p1: Expression, p2: Expression) extends Expression {

  type POINT = Map[String, Any]

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val points: Seq[POINT] = Seq(p1, p2).map {
      case Identifier(name) => ctx(name) match {
        case p: Point => p(ctx).asInstanceOf[POINT]
        case m: POINT => m
      }
      case p: Point => p(ctx).asInstanceOf[POINT]
    }
    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    if (HaversinCalculator.appliesTo(points(0), points(1)))
      HaversinCalculator.distance(points(0), points(1))
    else
      throw new IllegalArgumentException(s"Invalid points passed to distance($p1, $p2)")
  }

  def rewrite(f: (Expression) => Expression) = f(Distance(p1.rewrite(f), p2.rewrite(f)))

  def calculateType(symbols: SymbolTable): CypherType = CTFloat

  override def arguments: Seq[Expression] = p1.arguments ++ p2.arguments

  override def symbolTableDependencies: Set[String] = p1.symbolTableDependencies ++ p2.symbolTableDependencies

  override def toString = "Distance(" + p1 + ", " + p2 + ")"
}

trait DistanceCalculator {
  type POINT = Map[String, Any]
  def appliesTo(p1: POINT, p2: POINT): Boolean
  def distance(p1: POINT, p2: POINT): Double
}

object HaversinCalculator extends DistanceCalculator {

  val EARTH_RADIUS_METERS = 6378140.0

  def appliesTo(p1: POINT, p2: POINT): Boolean = {
    // TODO: determine CRS to be WGS84 for both points
    true
  }

  import Math._

  def coordinates(point: POINT): Seq[Double] = {
    Seq(point("longitude").asInstanceOf[Double], point("latitude").asInstanceOf[Double])
  }

  override def distance(p1: POINT, p2: POINT): Double = {
    val c1: Seq[Double] = coordinates(p1).map(toRadians)
    val c2: Seq[Double] = coordinates(p2).map(toRadians)
    val dx = c2(0) - c1(0)
    val dy = c2(1) - c1(1)
    val a = pow(sin(dy / 2), 2.0) + cos(c1(1)) * cos(c2(1)) * pow(sin(dx / 2.0), 2.0)
    val greatCircleDistance = 2.0 * atan2(sqrt(a), sqrt(1-a))
    EARTH_RADIUS_METERS * greatCircleDistance
  }
}