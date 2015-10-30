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
package org.neo4j.cypher.internal.compiler.v3_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphdb.PropertyContainer

case class PointFunction(data: Expression) extends NullInNullOutExpression(data) {

  def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState): Any = value match {
    case Identifier(name) => ctx(name)
    case n:PropertyContainer => Point(n)
    case m:LiteralMap =>
      val v = m(ctx)
      Point(v.asInstanceOf[Map[String, Any]])
    case m:Map[String, Any] => Point(m)
  }

  def rewrite(f: (Expression) => Expression) = f(PointFunction(data.rewrite(f)))

  def arguments = data.arguments

  def calculateType(symbols: SymbolTable): CypherType = CTPoint

  def symbolTableDependencies = data.symbolTableDependencies

  override def toString = "Point(" + data + ")"
}

object Point {
  def apply(entity: PropertyContainer): Point = (if (entity.hasProperty("crs")) entity.getProperty("crs") else None) match {
    case CRS.Cartesian => new CartesianPoint(entity.getProperty("x").asInstanceOf[Double], entity.getProperty("y").asInstanceOf[Double])
    case _ => new GeographicPoint(entity.getProperty("longitude").asInstanceOf[Double], entity.getProperty("latitude").asInstanceOf[Double], CRS.WGS84)
  }

  def apply(map: Map[String, Any]): Point = CRS.fromName(map.getOrElse("crs", CRS.WGS84.name).toString) match {
    case CRS.Cartesian => new CartesianPoint(map("x").asInstanceOf[Double], map("y").asInstanceOf[Double])
    case CRS.WGS84 => new GeographicPoint(map("longitude").asInstanceOf[Double], map("latitude").asInstanceOf[Double], CRS.WGS84)
    case x => throw new UnsupportedOperationException("Unsupported SRC type of value: " + x)
  }
}

case class CRSType(name: String)

object CRSType {
  val Projected = CRSType("projected")
  val Geographic = CRSType("geographic")
}

case class CRS(name: String, id: Int, crs_type: CRSType)

object CRS {
  val Cartesian = CRS("cartesian", 7203, CRSType.Projected) // See http://spatialreference.org/ref/sr-org/7203/
  val WGS84 = CRS("WGS-84", 4326, CRSType.Geographic)       // See http://spatialreference.org/ref/epsg/4326/

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

trait Geometry {
  def coordinates: Seq[Double]
  def crs: CRS
}

trait Point extends Geometry {
  def x: Double
  def y: Double
  def coordinates: Seq[Double] = Seq(x, y)
}

case class CartesianPoint(x: Double, y: Double) extends Point {
  def crs = CRS.Cartesian
}

case class GeographicPoint(longitude: Double, latitude: Double, crs: CRS) extends Point {
  def x: Double = longitude
  def y: Double = latitude
}
