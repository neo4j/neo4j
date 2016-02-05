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
package org.neo4j.cypher.internal.frontend.v3_0.helpers

import java.util.{Map => JavaMap}
import java.util.{List => JavaList}
import scala.collection.JavaConverters._

object JavaValueCompatibility {

  def asDeepJavaMap[S](map: Map[S, Any]): JavaMap[S, Any] =
    if (map == null) null else Eagerly.immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[S, Any]

  def asDeepJavaValue(value: Any): Any =
    if (value == null) null else value match {
      case map: Map[_, _] => Eagerly.immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[_, _]
      case iterable: Iterable[_] => iterable.map(asDeepJavaValue).toSeq.asJava: JavaList[_]
      case anything => anything
    }
}
