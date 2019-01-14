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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.v3_4.LabelId
import org.neo4j.cypher.internal.v3_4.expressions.LabelName

case class LazyLabel(name: String) {
  private var id: Option[LabelId] = None

  def getOptId(context: TokenContext): Option[LabelId] = id match {
    case None => {
      id = context.getOptLabelId(name).map(LabelId)
      id
    }
    case x => x
  }

  def getOrCreateId(context: QueryContext): LabelId = id match {
    case None => {
      val labelId = LabelId(context.getOrCreateLabelId(name))
      id = Some(labelId)
      labelId
    }
    case Some(x) => x
  }

  // yuck! this is only used by tests...
  def id(table: SemanticTable): Option[LabelId] = id match {
    case None => {
      id = table.resolvedLabelNames.get(name)
      id
    }
    case x => x
  }
}

object LazyLabel {
  def apply(name: LabelName)(implicit table: SemanticTable): LazyLabel = {
    val label = new LazyLabel(name.name)
    label.id = table.id(name)
    label
  }
}
