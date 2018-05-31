/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, PathWrappingPathValue, RelationshipProxyWrappingValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.{ListValue, MapValue}

import scala.collection.mutable

object valueHelper {
  def fromValue(value: AnyValue): Any = value match {
    case s: TextValue => s.stringValue()
    case b: BooleanValue => b.booleanValue()
    case d: FloatingPointValue => d.doubleValue()
    case d: IntegralValue => d.longValue()

    case m: MapValue => {
      val map: mutable.Map[String, Any] = mutable.Map[String, Any]()
      m.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, fromValue(u))
      })
      map.toMap
    }
    case n: NodeProxyWrappingNodeValue => n.nodeProxy()
    case r: RelationshipProxyWrappingValue => r.relationshipProxy()
    case p: PathWrappingPathValue => p.path()
    case a: ArrayValue => a.asObjectCopy()
    case a: ListValue => Vector(a.asArray().map(fromValue): _*)
    case Values.NO_VALUE => null
  }

}
