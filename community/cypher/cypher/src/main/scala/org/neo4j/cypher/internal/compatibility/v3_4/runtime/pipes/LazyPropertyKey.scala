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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.neo4j.cypher.internal.compiler.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_4.PropertyKeyId
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_4.expressions.PropertyKeyName

case class LazyPropertyKey(name: String) {
  private var id: Option[PropertyKeyId] = None

  def id(context: TokenContext): Option[PropertyKeyId] = id match {
    case None =>
      id = context.getOptPropertyKeyId(name).map(PropertyKeyId)
      id
    case x => x
  }
}

object LazyPropertyKey {

  def apply(name: PropertyKeyName)(implicit table: SemanticTable): LazyPropertyKey = {
    val property = new LazyPropertyKey(name.name)
    property.id = table.id(name)
    property
  }
}



