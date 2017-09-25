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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_3.ast.RelTypeName
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

case class LazyTypes(names: Array[String]) {

  private var ids = Array.empty[Int]

  def types(context: QueryContext): Option[Array[Int]] = if (names.isEmpty) None else {
    if (ids.length != names.length) {
      ids = names.flatMap(context.getOptRelTypeId)
    }
    Some(ids)
  }
}

object LazyTypes {
  def apply(names: Array[RelTypeName])(implicit table:SemanticTable): LazyTypes = {
    val types = LazyTypes(names.map(_.name))
    types.ids = names.flatMap(_.id).map(_.id)
    types
  }
  val empty = LazyTypes(Array.empty[String])
}
