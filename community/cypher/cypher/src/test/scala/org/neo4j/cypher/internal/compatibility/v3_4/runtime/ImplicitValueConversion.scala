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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.PathImpl
import org.neo4j.graphdb.{Node, Path, Relationship}
import org.neo4j.helpers.ValueUtils
import org.neo4j.values._
import org.neo4j.values.storable.Values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues._
import org.neo4j.values.virtual._

import scala.collection.JavaConverters._

object ImplicitValueConversion {

  implicit def toListValue(s: Seq[_]): ListValue = list(s.map(ValueUtils.of): _*)

  implicit def toListValue(list: java.util.List[_]): ListValue = ValueUtils.asListValue(list)

  implicit def toStringValue(s: String): TextValue = stringValue(s)

  implicit def toStringArrayValue(s: Array[String]): ArrayValue = stringArray(s:_*)

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

  implicit def toNodeValue(n: Node): NodeValue = ValueUtils.fromNodeProxy(n)

  implicit def toEdgeValue(r: Relationship): EdgeValue = ValueUtils.fromRelationshipProxy(r)

  implicit def toPathValue(p: Path): PathValue = ValueUtils.asPathValue(p)

  implicit def toPathValue(p: PathImpl): PathValue = ValueUtils.asPathValue(p)

  implicit def toListValue(t: TraversableOnce[_]): ListValue =
    ValueUtils.asListValue(t.toIterable.asJava)

  implicit def toValueTuple(t: (String, Any)): (String, AnyValue) = (t._1, ValueUtils.of(t._2))
}
