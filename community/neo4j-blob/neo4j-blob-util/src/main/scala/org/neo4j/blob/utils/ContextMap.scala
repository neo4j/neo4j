package org.neo4j.blob.utils

import scala.collection.mutable.{Map => MMap}
/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

object GlobalContext extends ContextMap {

}

class ContextMap {
  private val _map = MMap[String, Any]();

  def put[T](key: String, value: T): T = {
    _map(key) = value
    value
  };

  def put[T](value: T)(implicit manifest: Manifest[T]): T = put[T](manifest.runtimeClass.getName, value)

  def get[T](key: String): T = {
    _map(key).asInstanceOf[T]
  };

  def getOption[T](key: String): Option[T] = _map.get(key).map(_.asInstanceOf[T]);

  def get[T]()(implicit manifest: Manifest[T]): T = get(manifest.runtimeClass.getName);

  def getOption[T]()(implicit manifest: Manifest[T]): Option[T] = getOption(manifest.runtimeClass.getName);
}
