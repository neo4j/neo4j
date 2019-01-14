/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import java.time._
import java.time.temporal.TemporalAmount

import org.neo4j.cypher.internal.util.v3_4.Eagerly
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.graphdb.spatial.{Geometry, Point}
import org.neo4j.graphdb.{Node, Path, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.fromArray
import org.neo4j.values.virtual.{MapValue, VirtualValues}

import scala.collection.JavaConverters._

object ValueConversion {
  def getValueConverter(cType: CypherType): Any => AnyValue = {
    val converter: Any => AnyValue = cType match {
      case CTNode => n => ValueUtils.fromNodeProxy(n.asInstanceOf[Node])
      case CTRelationship => r => ValueUtils.fromRelationshipProxy(r.asInstanceOf[Relationship])
      case CTBoolean => b => Values.booleanValue(b.asInstanceOf[Boolean])
      case CTFloat => d => Values.doubleValue(d.asInstanceOf[Double])
      case CTInteger => l => Values.longValue(l.asInstanceOf[Long])
      case CTNumber => l => Values.numberValue(l.asInstanceOf[Number])
      case CTString => l => Values.stringValue(l.asInstanceOf[String])
      case CTPath => p => ValueUtils.fromPath(p.asInstanceOf[Path])
      case CTMap => m => ValueUtils.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])
      case ListType(_)  => {
        case a: Array[Byte] => Values.byteArray(a) // procedures can produce byte[] as a valid output type
        case l => ValueUtils.asListValue(l.asInstanceOf[java.util.Collection[_]])
      }
      case CTAny => o => ValueUtils.of(o)
      case CTPoint => o => ValueUtils.asPointValue(o.asInstanceOf[Point])
      case CTGeometry => o => ValueUtils.asGeometryValue(o.asInstanceOf[Geometry])
      case CTDateTime => o => DateTimeValue.datetime(o.asInstanceOf[ZonedDateTime])
      case CTLocalDateTime => o => LocalDateTimeValue.localDateTime(o.asInstanceOf[LocalDateTime])
      case CTDate => o => DateValue.date(o.asInstanceOf[LocalDate])
      case CTTime => o => TimeValue.time(o.asInstanceOf[OffsetTime])
      case CTLocalTime => o => LocalTimeValue.localTime(o.asInstanceOf[LocalTime])
      case CTDuration => o => Values.durationValue(o.asInstanceOf[TemporalAmount])
    }

    (v) => if (v == null) Values.NO_VALUE else converter(v)
  }

  def asValues(params: Map[String, Any]): MapValue = VirtualValues.map(Eagerly.immutableMapValues(params, asValue).asJava)
  def asValue(value: Any): AnyValue = value match {
    case null => Values.NO_VALUE
    case s: String => Values.stringValue(s)
    case c: Char => Values.charValue(c)
    case d: Double => Values.doubleValue(d)
    case f: Float => Values.doubleValue(f)
    case n: Number => Values.longValue(n.longValue())
    case b: Boolean => Values.booleanValue(b)
    case n: Node => ValueUtils.fromNodeProxy(n)
    case r: Relationship => ValueUtils.fromRelationshipProxy(r)
    case p: Path => ValueUtils.fromPath(p)
    case p: Point => ValueUtils.asPointValue(p)
    case p: Geometry => ValueUtils.asGeometryValue(p)
    case x: ZonedDateTime => DateTimeValue.datetime(x)
    case x: LocalDateTime => LocalDateTimeValue.localDateTime(x)
    case x: LocalDate => DateValue.date(x)
    case x: OffsetTime => TimeValue.time(x)
    case x: LocalTime => LocalTimeValue.localTime(x)
    case x: TemporalAmount => Values.durationValue(x)
    case m: Map[_, _] => VirtualValues.map(Eagerly.immutableMapValues(m.asInstanceOf[Map[String, Any]], asValue).asJava)
    case m: java.util.Map[_, _] => ValueUtils.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])
    case a: TraversableOnce[_] => VirtualValues.list(a.map(asValue).toArray:_*)
    case c: java.util.Collection[_] => ValueUtils.asListValue(c)
    case a: Array[_] =>
      a.getClass.getComponentType.getName match {
      case "byte" => byteArray(a.asInstanceOf[Array[Byte]]) // byte[] is supported in procedures and BOLT
      case "short" => fromArray(Values.shortArray(a.asInstanceOf[Array[Short]]))
      case "char" => fromArray(Values.charArray(a.asInstanceOf[Array[Char]]))
      case "int" => fromArray(Values.intArray(a.asInstanceOf[Array[Int]]))
      case "long" => fromArray(Values.longArray(a.asInstanceOf[Array[Long]]))
      case "float" => fromArray(Values.floatArray(a.asInstanceOf[Array[Float]]))
      case "double" => fromArray(Values.doubleArray(a.asInstanceOf[Array[Double]]))
      case "boolean" => fromArray(Values.booleanArray(a.asInstanceOf[Array[Boolean]]))
      case "java.lang.String" => fromArray(Values.stringArray(a.asInstanceOf[Array[String]]:_*))
      case _ => VirtualValues.list(a.map(asValue):_*)
    }


  }
}
