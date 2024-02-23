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
package org.neo4j.cypher.internal.runtime

import org.neo4j.values.AnyValue

case class ParameterMapping(private val mapping: Map[String, OffsetAndDefault] = Map.empty) {
  def size: Int = mapping.size
  def nonEmpty: Boolean = mapping.nonEmpty
  def foreach[U](f: (String, OffsetAndDefault) => U): Unit = mapping.foreach(m => f(m._1, m._2))
  def contains(key: String): Boolean = mapping.contains(key)
  def offsetFor(key: String): Int = mapping(key).offset
  def defaultValueFor(key: String): Option[AnyValue] = mapping(key).default

  def updated(key: String): ParameterMapping =
    copy(mapping = mapping.updated(key, mapping.getOrElse(key, OffsetAndDefault(mapping.size, None))))

  def updated(key: String, default: AnyValue): ParameterMapping =
    copy(mapping = mapping.updated(key, mapping.getOrElse(key, OffsetAndDefault(mapping.size, Some(default)))))
}

object ParameterMapping {
  val empty: ParameterMapping = ParameterMapping()
}

case class OffsetAndDefault(offset: Int, default: Option[AnyValue])
