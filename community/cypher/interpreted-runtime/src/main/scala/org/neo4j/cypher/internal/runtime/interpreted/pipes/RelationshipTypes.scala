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

abstract class RelationshipTypes {
  def types(context: ReadTokenContext): Array[Int]
}

final class EagerTypes(tokens: Array[Int]) extends RelationshipTypes {
  override def types(context: ReadTokenContext): Array[Int] = tokens
}

final class LazyTypes(names: Array[String], private var ids: Array[Int]) extends RelationshipTypes {

  override def types(context: ReadTokenContext): Array[Int] = {
    if (ids.length != names.length) {
      ids = names.flatMap(context.getOptRelTypeId)
    }
    ids
  }
}

object RelationshipTypes {

  def apply(names: Array[RelTypeName])(implicit table: SemanticTable): RelationshipTypes = {
    if (names.isEmpty) empty
    else {
      val ids = names.flatMap(r => table.id(r).map(_.id))
      if (ids.length == names.length) new EagerTypes(ids)
      else {
        val types = new LazyTypes(names.map(_.name), ids)
        types
      }
    }
  }

  def apply(names: Array[String], context: ReadTokenContext): RelationshipTypes = {
    if (names.isEmpty) empty
    else {
      val ids = names.flatMap(context.getOptRelTypeId)
      if (ids.length == names.length) new EagerTypes(ids)
      else {
        val types = new LazyTypes(names, ids)
        types
      }
    }
  }

  def apply(names: Array[String]): RelationshipTypes = {
    if (names.isEmpty) empty
    else new LazyTypes(names, Array.empty)
  }

  val empty: RelationshipTypes = new EagerTypes(null)
}
