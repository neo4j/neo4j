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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers

import org.neo4j.cypher.internal.frontend.v3_3.helpers.Eagerly
import org.neo4j.cypher.internal.frontend.v3_3.symbols
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.cypher.internal.javacompat.ValueUtils
import org.neo4j.graphdb.spatial.{Geometry, Point}
import org.neo4j.graphdb.{Node, Path, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.virtual.VirtualValues.fromArray
import org.neo4j.values.virtual.{MapValue, VirtualValues}

import scala.collection.JavaConverters._

object ValueConversion {
  def getValueConverter(cType: CypherType): Any => AnyValue = cType match {
    case symbols.CTNode => n => ValueUtils.fromNodeProxy(n.asInstanceOf[Node])
    case symbols.CTRelationship => r => ValueUtils.fromRelationshipProxy(r.asInstanceOf[Relationship])
    case symbols.CTBoolean => b => Values.booleanValue(b.asInstanceOf[Boolean])
    case symbols.CTFloat => d => Values.doubleValue(d.asInstanceOf[Double])
    case symbols.CTInteger => l => Values.longValue(l.asInstanceOf[Long])
    case symbols.CTNumber => l => Values.numberValue(l.asInstanceOf[Number])
    case symbols.CTString => l => Values.stringValue(l.asInstanceOf[String])
    case symbols.CTPath => p => ValueUtils.asPathValue(p.asInstanceOf[Path])
    case symbols.CTMap => m => ValueUtils.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])
    case symbols.ListType(_)  => l => ValueUtils.asListValue(l.asInstanceOf[java.util.Collection[_]])
    case symbols.CTAny => o => ValueUtils.of(o)
    case symbols.CTPoint => o => ValueUtils.asPointValue(o.asInstanceOf[Point])
    case symbols.CTGeometry => o => ValueUtils.asPointValue(o.asInstanceOf[Geometry])
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
    case p: Path => ValueUtils.asPathValue(p)
    case p: Point => ValueUtils.asPointValue(p)
    case p: Geometry => ValueUtils.asPointValue(p)
    case m: Map[_, _] => VirtualValues.map(Eagerly.immutableMapValues(m.asInstanceOf[Map[String, Any]], asValue).asJava)
    case m: java.util.Map[_, _] => ValueUtils.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])
    case a: TraversableOnce[_] => VirtualValues.list(a.map(asValue).toArray:_*)
    case c: java.util.Collection[_] => ValueUtils.asListValue(c)
    case a: Array[_] =>
      a.getClass.getComponentType.getName match {
      case "byte" => fromArray(byteArray(a.asInstanceOf[Array[Byte]]))
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
