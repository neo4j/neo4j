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
import org.neo4j.cypher.internal.compiler.v3_0.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import scala.collection.JavaConverters._

case class PointFunction(data: Expression) extends NullInNullOutExpression(data) {

  override def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState): Any = value match {
    case IsMap(mapCreator) =>
      val map = mapCreator(state.query)
      map.getOrElse("crs", CRS.WGS84.name) match {
        case CRS.Cartesian.name =>
          val x = map("x").asInstanceOf[Double]
          val y = map("y").asInstanceOf[Double]
          new CartesianPoint(x, y)

        case CRS.WGS84.name =>
          val longitude = map("longitude").asInstanceOf[Double]
          val latitude = map("latitude").asInstanceOf[Double]
          new GeographicPoint(longitude, latitude, CRS.WGS84)
      }
    case x => throw new CypherTypeException(s"Expected a map but got $x")
  }

  override def rewrite(f: (Expression) => Expression) = f(PointFunction(data.rewrite(f)))

  override def arguments = data.arguments

  override def calculateType(symbols: SymbolTable) = CTPoint

  override def symbolTableDependencies = data.symbolTableDependencies

  override def toString = "Point(" + data + ")"
}

case class CRS(name: String, id: Int, link:String) extends ActsAsMap {
  def asMap = Map[String, Object]("name" -> name, "type" -> "link", "properties" -> Map( "href" -> s"${link}ogcwkt/", "type" -> "ogcwkt" ).asJava).asJava
}

object CRS {
  val Cartesian = CRS("cartesian", 7203, "http://spatialreference.org/ref/sr-org/7203/")
  val WGS84 = CRS("WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/")

  def fromName(name: String) = name match {
    case Cartesian.name => Cartesian
    case WGS84.name => WGS84
    case _ => throw new UnsupportedOperationException("Invalid or unsupported CRS name: " + name)
  }

  def fromSRID(id: Int) = id match {
    case Cartesian.id => Cartesian
    case WGS84.id => WGS84
    case _ => throw new UnsupportedOperationException("Invalid or unsupported SRID: " + id)
  }
}

trait ActsAsMap {
  def asMap: java.util.Map[String,Object]
}

trait Geometry extends ActsAsMap {
  def coordinates: Seq[Double]
  def crs: CRS
}

trait Point extends Geometry {
  def x: Double
  def y: Double
  def coordinates: Seq[Double] = Seq(x, y)

  def asMap = Map[String, Object]("type" -> "Point", "coordinates" -> coordinates.asJava, "crs" -> crs).asJava
}

case class CartesianPoint(x: Double, y: Double) extends Point {
  def crs = CRS.Cartesian
}

case class GeographicPoint(longitude: Double, latitude: Double, crs: CRS) extends Point {
  def x: Double = longitude
  def y: Double = latitude
}
