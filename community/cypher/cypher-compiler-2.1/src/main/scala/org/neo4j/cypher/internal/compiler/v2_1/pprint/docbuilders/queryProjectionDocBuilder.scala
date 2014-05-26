/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryProjection
import org.neo4j.cypher.internal.compiler.v2_1.ast.{DescSortItem, AscSortItem}

case class queryProjectionDocBuilder(prefix: String = "WITH") extends CachingDocBuilder[Any] {
  import Doc._

  override protected def newNestedDocGenerator = {
    case queryProjection: QueryProjection => (inner: DocGenerator[Any]) =>
      val projectionMapDoc = queryProjection.projections.collect {
        case (k, v) => group( inner(v) :/: "AS " :: s"`$k`" )
      }
      val projection = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))

      val sortItemDocs = queryProjection.sortItems.collect {
        case AscSortItem(expr)  => inner(expr)
        case DescSortItem(expr) => inner(expr) :/: "DESC"
      }
      val sortItems = if (sortItemDocs.isEmpty) nil else group("ORDER BY" :/: sepList(sortItemDocs))

      val skip = queryProjection.skip.map( skip => group("SKIP" :/: inner(skip)) ).getOrElse(nil)
      val limit = queryProjection.limit.map( limit => group("LIMIT" :/: inner(limit)) ).getOrElse(nil)

      section(prefix, projection :+: sortItems :+: skip :+: limit)
  }
}
