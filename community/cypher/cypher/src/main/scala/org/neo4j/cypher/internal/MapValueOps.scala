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
package org.neo4j.cypher.internal

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

object MapValueOps {

  implicit class Ops(mv: MapValue) extends Map[String, AnyValue] {

    def getOption(key: String): Option[AnyValue] = mv.get(key) match {
      case _: NoValue => None
      case value      => Some(value)
    }

    override def get(key: String): Option[AnyValue] = getOption(key)

    override def iterator: Iterator[(String, AnyValue)] = {
      val keys = mv.keySet().iterator()
      new Iterator[(String, AnyValue)] {
        override def hasNext: Boolean = keys.hasNext

        override def next(): (String, AnyValue) = {
          val k = keys.next()
          (k, mv.get(k))
        }
      }
    }

    /*
  This is not very efficient, probably you don't want to use it for anything performance
  critical.
     */
    override def removed(key: String): Map[String, AnyValue] = {
      val mvb = new MapValueBuilder()
      mv.foreach((k, v) => if (!k.equals(key)) mvb.add(k, v))
      mvb.build()
    }

    override def updated[V1 >: AnyValue](key: String, value: V1): Map[String, V1] =
      mv.updatedWith(VirtualValues.map(Array(key), Array(value.asInstanceOf[AnyValue])))

    def optionallyUpdatedWith(key: String, converterOpt: Option[MapValue => AnyValue]): MapValue = {
      converterOpt.map(value => mv.updatedWith(key, value(mv))).getOrElse(mv)
    }

    def optionallyUpdatedWithValue(key: String, valueOpt: Option[AnyValue]): MapValue = {
      optionallyUpdatedWith(key, valueOpt.map(v => (_: MapValue) => v))
    }
  }
}
