/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.helpers

import collection.Map
import java.util.{Map => JavaMap}
import collection.JavaConverters._

object IsMap extends MapSupport {
  def unapply(x: Any): Option[Map[String, Any]] = if (isMap(x)) {
    Some(castToMap(x))
  } else {
    None
  }
}

trait MapSupport {
  def isMap(x: Any) = castToMap.isDefinedAt(x)

  def castToMap: PartialFunction[Any, Map[String,Any]] = {
    case x: Map[String, Any]       => x
    case x: JavaMap[String, Any]   => x.asScala
  }
}