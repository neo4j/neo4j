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
package org.neo4j.cypher.internal.compiler.v3_1

import org.neo4j.cypher.internal.frontend.v3_1.InvalidArgumentException

trait Geometry {
  def coordinates: Seq[Double]
  def crs: CRS
}

trait Point extends Geometry {
  def x: Double
  def y: Double
  def coordinates = Vector(x, y)
}

case class CartesianPoint(x: Double, y: Double, crs: CRS) extends Point

case class GeographicPoint(longitude: Double, latitude: Double, crs: CRS) extends Point {
  def x: Double = longitude
  def y: Double = latitude
}

case class CRS(name: String, code: Int, url: String)

object CRS {
  val Cartesian = CRS("cartesian", 7203, "http://spatialreference.org/ref/sr-org/7203/")
  val WGS84 = CRS("WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/")

  def fromName(name: String) = name match {
    case Cartesian.name => Cartesian
    case WGS84.name => WGS84
    case _ => throw new InvalidArgumentException(s"'$name' is not a supported coordinate reference system for points, supported CRS are: '${WGS84.name}', '${Cartesian.name}'")
  }

  def fromSRID(id: Int) = id match {
    case Cartesian.`code` => Cartesian
    case WGS84.`code` => WGS84
    case _ => throw new InvalidArgumentException(s"SRID '$id' does not match any supported coordinate reference system for points, supported CRS are: '${WGS84.name}', '${Cartesian.name}'")
  }
}
