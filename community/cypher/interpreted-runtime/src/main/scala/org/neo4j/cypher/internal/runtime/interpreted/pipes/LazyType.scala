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
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.WriteQueryContext
import org.neo4j.internal.kernel.api.TokenWrite

case class LazyType(name: String) {

  private var id = LazyType.UNKNOWN

  def getOrCreateType(context: WriteQueryContext): Int = {
    if (id == LazyType.UNKNOWN) {
      id = context.getOrCreateRelTypeId(name)
    }
    id
  }

  def getOrCreateType(token: TokenWrite): Int = {
    if (id == LazyType.UNKNOWN) {
      id = token.relationshipTypeGetOrCreateForName(name)
    }
    id
  }

  def getId(context: ReadTokenContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOptRelTypeId(name).getOrElse(LazyType.UNKNOWN)
    }
    id
  }
}

object LazyType {
  val UNKNOWN: Int = -1

  def apply(relTypeName: RelTypeName)(implicit table: SemanticTable): LazyType = {
    val typ = LazyType(relTypeName.name)
    typ.id = table.id(relTypeName)
    typ
  }
}
