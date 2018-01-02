/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.frontend.v2_3.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_3.spi.{TokenContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, LabelId}

case class LazyLabel(name:String) {
  private var id : Option[LabelId] = None

  def id(context: TokenContext): Option[LabelId] = id match {
    case None => {
      id = context.getOptLabelId(name).map(LabelId)
      id
    }
    case x    => x
  }

  // yuck! this is only used by tests...
  def id(table:SemanticTable):Option[LabelId] = id match {
    case None => {
      id = table.resolvedLabelIds.get(name)
      id
    }
    case x    => x
  }
}

object LazyLabel {
  def apply(name: LabelName)(implicit table:SemanticTable): LazyLabel = {
    val label = new LazyLabel(name.name)
    label.id = name.id
    label
  }
}
