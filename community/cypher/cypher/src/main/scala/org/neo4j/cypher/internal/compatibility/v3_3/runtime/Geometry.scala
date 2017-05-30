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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import java.util.Collections

import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InvalidArgumentException}
import org.neo4j.graphdb.spatial
import org.neo4j.graphdb.spatial.{Coordinate, Point}

import scala.beans.BeanProperty

abstract class ScalaPoint extends Point {
  def x: Double
  def y: Double
  def getCoordinates:java.util.List[Coordinate] =
    Collections.singletonList(new Coordinate(x,y))

  @BeanProperty
  val geometryType: String = "Point"
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
  def fromMap(map: collection.Map[String, Any]): Point = {
    if (map.contains("x") && map.contains("y")) {
      val x = safeToDouble(map("x"))
      val y = safeToDouble(map("y"))
      val crsName = map.getOrElse("crs", CRS.Cartesian.name).asInstanceOf[String]
      val crs = CRS.fromName(crsName)
      crs match {
        case CRS.WGS84 => GeographicPoint(x, y, crs)
        case _ => CartesianPoint(x, y, crs)
      }
    } else if (map.contains("latitude") && map.contains("longitude")) {
      val crsName = map.getOrElse("crs", CRS.WGS84.name).asInstanceOf[String]
      if (crsName != CRS.WGS84.name) throw new InvalidArgumentException(s"'$crsName' is not a supported coordinate reference system for geographic points, supported CRS are: '${CRS.WGS84.name}'")
      val latitude = safeToDouble(map("latitude"))
      val longitude = safeToDouble(map("longitude"))
      GeographicPoint(longitude, latitude, CRS.fromName(crsName))
    } else {
      throw new InvalidArgumentException("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'")
    }
  }
  private def safeToDouble(value: Any) = value match {
    case n: Number => n.doubleValue()
    case other => throw new CypherTypeException(other.getClass.getSimpleName + " is not a valid coordinate type.")
  }
}