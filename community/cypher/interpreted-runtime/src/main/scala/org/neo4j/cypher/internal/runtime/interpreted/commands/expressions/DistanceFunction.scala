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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import java.lang.Math._

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

case class DistanceFunction(p1: Expression, p2: Expression) extends Expression {

  private val availableCalculators = Seq(HaversinCalculator, CartesianCalculator)

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    (p1(ctx, state), p2(ctx, state)) match {
      case (geometry1: PointValue, geometry2: PointValue) => calculateDistance(geometry1, geometry2)
      case _ => Values.NO_VALUE
    }
  }

  def calculateDistance(geometry1: PointValue, geometry2: PointValue): AnyValue = {
    availableCalculators.collectFirst {
      case distance: DistanceCalculator if distance.isDefinedAt(geometry1, geometry2) =>
        Values.doubleValue(distance(geometry1, geometry2))
    }.getOrElse(
      Values.NO_VALUE
    )
  }

  override def rewrite(f: (Expression) => Expression) = f(DistanceFunction(p1.rewrite(f), p2.rewrite(f)))

  override def arguments: Seq[Expression] = p1.arguments ++ p2.arguments

  override def symbolTableDependencies = p1.symbolTableDependencies ++ p2.symbolTableDependencies

  override def toString = "Distance(" + p1 + ", " + p2 + ")"
}

trait DistanceCalculator extends PartialFunction[(PointValue, PointValue), Double] {
  def boundingBox(p1: PointValue, distance: Double): (PointValue, PointValue)
}

object CartesianCalculator extends DistanceCalculator {
  override def isDefinedAt(points: (PointValue, PointValue)): Boolean =
    points._1.getCoordinateReferenceSystem.getCode() == CoordinateReferenceSystem.Cartesian.getCode() &&
      points._2.getCoordinateReferenceSystem.getCode() == CoordinateReferenceSystem.Cartesian.getCode() &&
      points._1.coordinate().length == points._2.coordinate().length

  override def apply(points: (PointValue, PointValue)): Double = {
    val p1Coordinates = points._1.coordinate()
    val p2Coordinates = points._2.coordinate()
    val sqrSum = p1Coordinates.zip(p2Coordinates).foldLeft(0.0) { (sum,pair) =>
      val diff = pair._1 - pair._2
      sum + diff * diff
    }
    sqrt(sqrSum)
  }

  override def boundingBox(p: PointValue, distance: Double): (PointValue, PointValue) = {
    val coordinates = p.coordinate()
    val min = Values.pointValue(p.getCoordinateReferenceSystem, coordinates(0) - distance, coordinates(1) - distance)
    val max = Values.pointValue(p.getCoordinateReferenceSystem, coordinates(0) + distance, coordinates(1) + distance)
    (min, max)
  }
}

object HaversinCalculator extends DistanceCalculator {

  val EARTH_RADIUS_METERS = 6378140.0
  val EXTENSION_FACTOR = 1.0001

  override def isDefinedAt(points: (PointValue, PointValue)): Boolean =
    points._1.getCoordinateReferenceSystem.getCode() == CoordinateReferenceSystem.WGS84.getCode() &&
      points._2.getCoordinateReferenceSystem.getCode() == CoordinateReferenceSystem.WGS84.getCode()

  override def apply(points: (PointValue, PointValue)): Double = {
    val c1Coord = points._1.coordinate()
    val c2Coord = points._2.coordinate()
    val c1: Array[Double] = Array(toRadians(c1Coord(0)), toRadians(c1Coord(1)))
    val c2: Array[Double] = Array(toRadians(c2Coord(0)), toRadians(c2Coord(1)))
    val dx = c2(0) - c1(0)
    val dy = c2(1) - c1(1)
    val a = pow(sin(dy / 2), 2.0) + cos(c1(1)) * cos(c2(1)) * pow(sin(dx / 2.0), 2.0)
    val greatCircleDistance = 2.0 * atan2(sqrt(a), sqrt(1 - a))
    EARTH_RADIUS_METERS * greatCircleDistance
  }

  // http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
  // But calculating in degrees instead of radians to avoid rounding errors
  override def boundingBox(p: PointValue, distance: Double): (PointValue, PointValue) = {
    if (distance == 0.0) {
      return (p, p)
    }

    // Extend the distance slightly to assure that all relevant points lies inside the bounding box,
    // with rounding errors taken into account
    val extended_distance = distance * EXTENSION_FACTOR

    val crs = p.getCoordinateReferenceSystem
    val lat = p.coordinate()(1)
    val lon = p.coordinate()(0)

    val r = extended_distance / EARTH_RADIUS_METERS

    val lat_min = lat - toDegrees(r)
    val lat_max = lat + toDegrees(r)

    // If your query circle includes one of the poles
    if (lat_max >= 90) {
      val bottomLeft = Values.pointValue(crs, -180, lat_min)
      val topRight = Values.pointValue(crs, 180, 90)
      return (bottomLeft, topRight)
    } else if (lat_min <= -90) {
      val bottomLeft = Values.pointValue(crs, -180, -90)
      val topRight = Values.pointValue(crs, 180, lat_max)
      return (bottomLeft, topRight)
    }

    val delta_lon = toDegrees(asin(sin(r) / cos(toRadians(lat))))
    val lon_min = lon - delta_lon
    val lon_max = lon + delta_lon

    // If you query circle wraps around the dateline
    // Large rectangle covering all longitudes
    // TODO implement two rectangle solution instead
    if (lon_min < -180 || lon_max > 180) {
      val bottomLeft = Values.pointValue(crs, -180, lat_min)
      val topRight = Values.pointValue(crs, 180, lat_max)
      return (bottomLeft, topRight)
    }

    val bottomLeft = Values.pointValue(crs, lon_min, lat_min)
    val topRight = Values.pointValue(crs, lon_max, lat_max)

    (bottomLeft, topRight)
  }
}
