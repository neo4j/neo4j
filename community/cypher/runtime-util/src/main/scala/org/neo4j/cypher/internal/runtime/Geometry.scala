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
package org.neo4j.cypher.internal.runtime

import java.util.Collections

import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, InvalidArgumentException}
import org.neo4j.graphdb.spatial
import org.neo4j.graphdb.spatial.{Coordinate, Point}
import org.neo4j.values.storable.CoordinateReferenceSystem

import scala.beans.BeanProperty

abstract class ScalaPoint extends Point {
  def x: Double
  def y: Double
  def getCoordinates:java.util.List[Coordinate] =
    Collections.singletonList(new Coordinate(x,y))

  @BeanProperty
  val geometryType: String = "Point"

  // Different versions of Neo4j return different implementations of Point, so we need
  // This to ensure compatibility tests pass when comparing between versions.
  override def equals(other: Any): Boolean = {
    if (other == null) {
      return false
    }
    other match {
      case otherPoint: Point =>
        if (!otherPoint.getGeometryType.equals(this.getGeometryType)) return false
        if (!otherPoint.getCRS.getHref.equals(this.getCRS.getHref)) return false
        val otherCoord = otherPoint.getCoordinate.getCoordinate
        val thisCoord = this.getCoordinate.getCoordinate
        otherCoord == thisCoord
      case _ =>
        false
    }
  }
}

case class CartesianPoint(@BeanProperty x: Double,
                          @BeanProperty y: Double,
                          @BeanProperty CRS: CRS) extends ScalaPoint

case class GeographicPoint(longitude: Double, latitude: Double,
                           @BeanProperty CRS: CRS) extends ScalaPoint {

  @BeanProperty
  def x: Double = longitude

  @BeanProperty
  def y: Double = latitude
}

//TODO: These classes could be removed since CoordinateReferenceSystem implements the public API too
case class CRS(@BeanProperty name: String, @BeanProperty code: Int, @BeanProperty href: String) extends spatial.CRS {
  override def getType: String = name
}

object CRS {

  val Cartesian = CRS("cartesian", 7203, "http://spatialreference.org/ref/sr-org/7203/")
  val WGS84 = CRS("WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/")

  def fromName(name: String): CRS = name match {
    case Cartesian.name => Cartesian
    case WGS84.name => WGS84
    case _ => throw new InvalidArgumentException(s"'$name' is not a supported coordinate reference system for points, supported CRS are: '${WGS84.name}', '${Cartesian.name}'")
  }

  def fromSRID(id: Int): CRS = id match {
    case Cartesian.`code` => Cartesian
    case WGS84.`code` => WGS84
    case _ => throw new InvalidArgumentException(s"SRID '$id' does not match any supported coordinate reference system for points, supported CRS are: '${WGS84.code}', '${Cartesian.code}'")
  }

  def fromURL(url: String): CRS = url match {
    case Cartesian.`href` => Cartesian
    case WGS84.`href` => WGS84
    case _ => throw new InvalidArgumentException(s"HREF '$url' does not match any supported coordinate reference system for points, supported CRS are: '${WGS84.href}', '${Cartesian.href}'")
  }
}

object Points {
  // TODO: Is this necessary, perhaps PointValue is sufficient for use in Cypher-land
  def fromValue(crsValue: CoordinateReferenceSystem, coordinate: Array[Double]) = {
    val crs = CRS.fromSRID(crsValue.code)
    if (crs == CRS.WGS84) {
      new GeographicPoint(coordinate(0), coordinate(1), crs)
    } else {
      new CartesianPoint(coordinate(0), coordinate(1), crs)
    }
  }
  private def safeToDouble(value: Any) = value match {
    case n: Number => n.doubleValue()
    case other => throw new CypherTypeException(other.getClass.getSimpleName + " is not a valid coordinate type.")
  }
}
