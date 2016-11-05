package org.neo4j.cypher.internal.compatibility.v3_2

import java.util.Collections

import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.{CRS, Coordinate, Geometry, Point}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.Eagerly
import org.neo4j.graphdb.spatial

import scala.collection.JavaConverters._

object typeConversions extends RuntimeTypeConverter {
  override def asPublicType = {
    case point: Point => asPublicPoint(point)
    case geometry: Geometry => asPublicGeometry(geometry)
    case other => other
  }

  override def asPrivateType = {
    case map: Map[_, _] => asPrivateMap(map.asInstanceOf[Map[String, Any]])
    case seq: Seq[_] => seq.map(asPrivateType)
    case javaMap: java.util.Map[_, _] => Eagerly.immutableMapValues(javaMap.asScala, asPrivateType)
    case javaIterable: java.lang.Iterable[_] => javaIterable.asScala.map(asPrivateType)
    case arr: Array[Any] => arr.map(asPrivateType)
    case point: spatial.Point => asPrivatePoint(point)
    case geometry: spatial.Geometry => asPrivateGeometry(geometry)
    case value => value
  }

  private def asPublicPoint(point: Point) = new spatial.Point {
    override def getGeometryType = "Point"

    override def getCRS: spatial.CRS = asPublicCRS(point.crs)

    override def getCoordinates: java.util.List[spatial.Coordinate] = Collections
      .singletonList(new spatial.Coordinate(point.coordinate.values: _*))
  }

  private def asPublicGeometry(geometry: Geometry) = new spatial.Geometry {
    override def getGeometryType: String = geometry.geometryType

    override def getCRS: spatial.CRS = asPublicCRS(geometry.crs)

    override def getCoordinates = geometry.coordinates.map { c =>
      new spatial.Coordinate(c.values: _*)
    }.toIndexedSeq.asJava
  }

  private def asPublicCRS(crs: CRS) = new spatial.CRS {
    override def getType: String = crs.name

    override def getHref: String = crs.url

    override def getCode: Int = crs.code
  }

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
