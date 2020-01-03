/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_1.{CRS, Coordinate, Geometry, Point}
import org.neo4j.cypher.internal.frontend.v3_1.helpers.Eagerly
import org.neo4j.graphdb.spatial
import org.neo4j.values.storable.{CoordinateReferenceSystem, Values}

import scala.collection.JavaConverters._

object typeConversions extends RuntimeTypeConverter {
  override def asPublicType: Any => Any = {
    case point: Point => asPublicPoint(point)
    case geometry: Geometry => throw new IllegalStateException("There are no non-point geometries in 3.1")
    case other => other
  }

  override def asPrivateType: Any => Any = {
    case map: Map[_, _] => asPrivateMap(map.asInstanceOf[Map[String, Any]])
    case seq: Seq[_] => seq.map(asPrivateType)
    case javaMap: java.util.Map[_, _] => Eagerly.immutableMapValues(javaMap.asScala, asPrivateType)
    case javaIterable: java.lang.Iterable[_] => javaIterable.asScala.map(asPrivateType)
    case arr: Array[Any] => arr.map(asPrivateType)
    case point: spatial.Point => asPrivatePoint(point)
    case geometry: spatial.Geometry => asPrivateGeometry(geometry)
    case value => value
  }

  private def asPublicPoint(point: Point): org.neo4j.graphdb.spatial.Point =
    Values.pointValue(CoordinateReferenceSystem.get(point.crs.url), point.coordinate.values:_*)

  def asPrivateMap(incoming: Map[String, Any]): Map[String, Any] =
    Eagerly.immutableMapValues[String,Any, Any](incoming, asPrivateType)

  private def asPrivatePoint(point: spatial.Point) = new Point {
    override def x: Double = point.getCoordinate.getCoordinate.get(0)

    override def y: Double = point.getCoordinate.getCoordinate.get(1)

    override def crs: CRS = CRS.fromURL(point.getCRS.getHref)
  }

  private def asPrivateCoordinate(coordinate: spatial.Coordinate) =
    Coordinate(coordinate.getCoordinate.asScala.map(v=>v.doubleValue()):_*)

  private def asPrivateGeometry(geometry: spatial.Geometry) = new Geometry {
    override def coordinates: Array[Coordinate] = geometry.getCoordinates.asScala.toArray.map(asPrivateCoordinate)

    override def crs: CRS = CRS.fromURL(geometry.getCRS.getHref)

    override def geometryType: String = geometry.getGeometryType
  }
}
