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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.v4_0.expressions.LabelName

class LazyLabel(val name: String) {

  private var id: Int = LazyLabel.UNKNOWN

  def getId(context: TokenContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOptLabelId(name).getOrElse(LazyLabel.UNKNOWN)
    }
    id
  }

  def getOrCreateId(context: QueryContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOrCreateLabelId(name)
    }
    id
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[LazyLabel]

  override def equals(other: Any): Boolean = other match {
    case that: LazyLabel =>
      (that canEqual this) &&
        name == that.name
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object LazyLabel {
  val UNKNOWN: Int = -1

  def apply(name: String): LazyLabel = new LazyLabel(name)

  def apply(name: LabelName)(implicit table: SemanticTable): LazyLabel = {
    val label = new LazyLabel(name.name)
    label.id = table.id(name)
    label
  }
}
