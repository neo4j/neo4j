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
package org.neo4j.cypher.internal.compiler.v3_1.helpers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._

object ScalaCompatibility {

  def asScalaCompatible(value: Any): Any = value match {
    case m: JavaMap[_, _] => m.asScala.toMap.map { case (k, v) => asScalaCompatible(k) -> asScalaCompatible(v) }
    case c: JavaIterable[_] => c.asScala.map(asScalaCompatible)
    case t: Traversable[_] => t.toIterable
    case anything => anything
  }
}
