/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.ByteValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.FloatValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.ShortValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values.booleanValue
import org.neo4j.values.storable.Values.byteArray
import org.neo4j.values.storable.Values.byteValue
import org.neo4j.values.storable.Values.charArray
import org.neo4j.values.storable.Values.doubleArray
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.floatArray
import org.neo4j.values.storable.Values.floatValue
import org.neo4j.values.storable.Values.intArray
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longArray
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.shortArray
import org.neo4j.values.storable.Values.shortValue
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues.list

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.language.implicitConversions

object ImplicitValueConversion {

  implicit def toListValue(s: Seq[_]): ListValue = list(s.map(ValueUtils.of): _*)

  implicit def toListValue(list: java.util.List[_]): ListValue = ValueUtils.asListValue(list)

  implicit def toStringValue(s: String): TextValue = stringValue(s)

  implicit def toStringArrayValue(s: Array[String]): ArrayValue = stringArray(s: _*)

  implicit def toByteArrayValue(s: Array[Byte]): ArrayValue = byteArray(s)

  implicit def toShortArrayValue(s: Array[Short]): ArrayValue = shortArray(s)

  implicit def toIntArrayValue(s: Array[Int]): ArrayValue = intArray(s)

  implicit def toLongArrayValue(s: Array[Long]): ArrayValue = longArray(s)

  implicit def toFloatArrayValue(s: Array[Float]): ArrayValue = floatArray(s)

  implicit def toDoubleArrayValue(s: Array[Double]): ArrayValue = doubleArray(s)

  implicit def toCharArrayValue(s: Array[Char]): ArrayValue = charArray(s)

  implicit def toBooleanValue(b: Boolean): BooleanValue = booleanValue(b)

  implicit def toIntValue(s: Int): IntValue = intValue(s)

  implicit def toShortValue(s: Short): ShortValue = shortValue(s)

  implicit def toByteValue(s: Byte): ByteValue = byteValue(s)

  implicit def toLongValue(s: Long): LongValue = longValue(s)

  implicit def toDoubleValue(s: Double): DoubleValue = doubleValue(s)

  implicit def toFloatValue(s: Float): FloatValue = floatValue(s)

  implicit def toMapValue(m: Map[String, _]): MapValue =
    ValueUtils.asMapValue(m.asJava.asInstanceOf[java.util.Map[String, AnyRef]])

  implicit def toMapValue(m: java.util.Map[String, Any]): MapValue =
    ValueUtils.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])

  implicit def toNodeValue(n: Node): VirtualNodeValue = ValueUtils.fromNodeEntity(n)

  implicit def toRelationshipValue(r: Relationship): VirtualRelationshipValue = ValueUtils.fromRelationshipEntity(r)

  implicit def toPathValue(p: Path): VirtualPathValue = ValueUtils.fromPath(p)

  implicit def toPathValue(p: PathImpl): VirtualPathValue = ValueUtils.fromPath(p)

  implicit def toListValue(t: IterableOnce[_]): ListValue =
    ValueUtils.asListValue(t.toIterable.asJava)

  implicit def toValueTuple(t: (String, Any)): (String, AnyValue) = (t._1, ValueUtils.of(t._2))
}
