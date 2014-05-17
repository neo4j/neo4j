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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryProjection
import org.neo4j.cypher.internal.compiler.v2_1.ast.{DescSortItem, AscSortItem}

case class QueryProjectionDocGenerator(prefix: String = "WITH") extends NestedDocGenerator[Any] {
  import Doc._

  val instance: RecursiveDocGenerator[Any] = {
    case queryProjection: QueryProjection => (inner: DocGenerator[Any]) =>
      val projectionMapDoc = queryProjection.projections.collect {
        case (k, v) => group( cons(inner(v), cons(breakHere, cons(text("AS "), cons(text(s"`$k`"))))) )
      }
      val projection = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc.toList))

      val sortItemDocs = queryProjection.sortItems.collect {
        case AscSortItem(expr)  => inner(expr)
        case DescSortItem(expr) => breakCons(inner(expr), cons(text("DESC")))
      }
      val sortItems = if (sortItemDocs.isEmpty) nil else group(breakCons(text("ORDER BY"), cons(sepList(sortItemDocs.toList))))

      val skip = queryProjection.skip.map( skip => group(breakCons(text("SKIP"), inner(skip))) ).getOrElse(nil)
      val limit = queryProjection.limit.map( limit => group(breakCons(text("LIMIT"), inner(limit))) ).getOrElse(nil)

      group(cons(text(prefix), nest(cons(breakHere, group(breakList(List(
        projection, sortItems, skip, limit
      ).filter(_ != NilDoc)))))))
  }
}
