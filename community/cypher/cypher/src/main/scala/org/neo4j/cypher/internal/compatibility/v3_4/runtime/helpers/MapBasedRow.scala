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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers

import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.Map

class MapBasedRow extends ResultRow {

  var map: Map[String, Any] = Map.empty

  override def getNode(key: String): Node = getWithType(key, classOf[Node])

  override def getRelationship(key: String): Relationship = getWithType(key, classOf[Relationship])

  override def get(key: String): Object = getWithType(key, classOf[Object])

  override def getNumber(key: String): Number = getWithType(key, classOf[Number])

  override def getBoolean(key: String): java.lang.Boolean = getWithType(key, classOf[java.lang.Boolean])

  override def getPath(key: String): Path = getWithType(key, classOf[Path])

  override def getString(key: String): String = getWithType(key, classOf[String])

  private def getWithType[T](key: String, clazz: Class[T]): T = {
    map.get(key) match {
      case None =>
        throw new IllegalArgumentException("No column \"" + key + "\" exists")
      case Some(value) if clazz.isInstance(value) || value == null =>
        clazz.cast(value)
      case Some(value) =>
        throw new NoSuchElementException(
          "The current item in column \"" + key + "\" is not a " + clazz + ": \"" + value + "\"")
    }
  }
}