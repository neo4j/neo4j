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
package org.neo4j.cypher.internal.compiler.v3_0.helpers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.frontend.v3_0.helpers.Eagerly.immutableMapValues

import scala.collection.JavaConverters._
import scala.collection.immutable

// Converts java runtime values to scala runtime values
//
// Main use: Converting results when using ExecutionEngine from scala
//
class RuntimeScalaValueConverter(skip: Any => Boolean) {

  final def asDeepScalaMap[A, B](map: JavaMap[A, B]): immutable.Map[A, Any] =
    if (map == null) null else immutableMapValues(map.asScala, asDeepScalaValue): immutable.Map[A, Any]

  final def asShallowScalaMap[A, B](map: JavaMap[A, B]): immutable.Map[A, Any] =
    if (map == null) null else map.asScala.toMap

  def asDeepScalaValue(value: Any): Any = value match {
    case anything if skip(anything) => anything
    case javaMap: JavaMap[_, _] => immutableMapValues(javaMap.asScala, asDeepScalaValue): immutable.Map[_, _]
    case javaIterable: JavaIterable[_] => javaIterable.asScala.map(asDeepScalaValue).toList: List[_]
    case map: collection.Map[_, _] => immutableMapValues(map, asDeepScalaValue): immutable.Map[_, _]
    case traversable: TraversableOnce[_] => traversable.map(asDeepScalaValue).toList: List[_]
    case anything => anything
  }

  def asShallowScalaValue(value: Any): Any = value match {
    case anything if skip(anything) => anything
    case javaMap: JavaMap[_, _] => javaMap.asScala.toMap: immutable.Map[_, _]
    case javaIterable: JavaIterable[_] => javaIterable.asScala.toList: List[_]
    case map: collection.Map[_, _] => map.toMap: immutable.Map[_, _]
    case anything => anything
  }
}
