/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import java.util

import org.neo4j.cypher.internal.util.Eagerly.immutableMapValues

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.Map

// This converts runtime scala values into runtime Java value
//
// Main use: Converting parameters when using ExecutionEngine from scala
//
class RuntimeJavaValueConverter(skip: Any => Boolean) {

  final def asDeepJavaMap[S](map: Map[S, Any]): util.Map[S, Any] =
    if (map == null) null else immutableMapValues(map, asDeepJavaValue).asJava: util.Map[S, Any]

  def asDeepJavaValue(value: Any): Any = value match {
    case anything if skip(anything) => anything
    case map: Map[_, _] => immutableMapValues(map, asDeepJavaValue).asJava: util.Map[_, _]
    case JavaListWrapper(inner, _) => inner
    case iterable: Iterable[_] => iterable.map(asDeepJavaValue).toIndexedSeq.asJava: util.List[_]
    case traversable: TraversableOnce[_] => traversable.map(asDeepJavaValue).toVector.asJava: util.List[_]
    case anything => anything
  }
}


