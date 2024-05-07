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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.WriteQueryContext
import org.neo4j.internal.kernel.api.TokenWrite

class LazyLabel(val name: String) {

  private var id: Int = LazyLabel.UNKNOWN

  def getId(context: ReadTokenContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOptLabelId(name).getOrElse(LazyLabel.UNKNOWN)
    }
    id
  }

  def getOrCreateId(context: WriteQueryContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOrCreateLabelId(name)
    }
    id
  }

  def getOrCreateId(token: TokenWrite): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = token.labelGetOrCreateForName(name)
    }
    id
  }

  override def equals(other: Any): Boolean = other match {
    case that: LazyLabel => name == that.name
    case _               => false
  }

  override def hashCode(): Int = name.hashCode
}

object LazyLabel {
  val UNKNOWN: Int = -1

  def apply(name: String): LazyLabel = new LazyLabel(name)

  def apply(name: LabelName)(implicit table: SemanticTable): LazyLabel = {
    val label = new LazyLabel(name.name)
    label.id = table.id(name)
    label
  }

  def apply(name: LabelName, context: ReadTokenContext): LazyLabel = {
    val label = new LazyLabel(name.name)
    label.getId(context)
    label
  }
}
