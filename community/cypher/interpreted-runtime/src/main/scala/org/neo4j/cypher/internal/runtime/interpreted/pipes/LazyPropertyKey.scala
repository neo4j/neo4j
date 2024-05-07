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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey.UNKNOWN

case class LazyPropertyKey(name: String) {
  private var id: Int = UNKNOWN

  def id(context: ReadTokenContext): Int = {
    if (id == UNKNOWN) {
      id = context.getOptPropertyKeyId(name).getOrElse(UNKNOWN)
    }
    id
  }
}

object LazyPropertyKey {
  val UNKNOWN: Int = -1

  def apply(name: PropertyKeyName)(implicit table: SemanticTable): LazyPropertyKey = {
    val property = new LazyPropertyKey(name.name)
    property.id = table.id(name)
    property
  }

  def apply(name: PropertyKeyName, context: ReadTokenContext): LazyPropertyKey = {
    val property = new LazyPropertyKey(name.name)
    property.id(context)
    property
  }
}
