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
package cypher.cucumber.db

import java.util.{List => JavaList, Map => JavaMap}

import org.neo4j.cypher.internal.frontend.v3_1.helpers.Eagerly._

import scala.collection.Map

object ParametersConverter {

  import scala.collection.JavaConverters._

  def apply(map: Map[String, Any]): JavaMap[String, AnyRef] = {
    val result = if (map == null) null else immutableMapValues(map, asDeepJavaValue).asJava
    result.asInstanceOf[JavaMap[String, AnyRef]]
  }

  private def asDeepJavaValue(value: Any): Any = value match {
    case map: Map[_, _] => immutableMapValues(map, asDeepJavaValue).asJava: JavaMap[_, _]
    case iterable: Iterable[_] => iterable.map(asDeepJavaValue).toVector.asJava: JavaList[_]
    case traversable: TraversableOnce[_] => traversable.map(asDeepJavaValue).toVector.asJava: JavaList[_]
    case anything => anything
  }
}
