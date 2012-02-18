/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal

import commands._


/*
This rewriter rewrites so that ordering
 */
object ReattachAliasedExpressions {
  def apply(q: Query): Query = {
    if (q.sort.isEmpty)
      q
    else {
      val newSort = Some(Sort(q.sort.get.sortItems.map(rewrite(q.returns.returnItems)):_*))
      Query(q.returns, q.start, q.matching, q.where, q.aggregation, newSort, q.slice, q.namedPaths, q.having, q.queryString)
    }
  }

  private def rewrite(returnItems: Seq[ReturnItem])(in: SortItem): SortItem = {
    val expression = in.expression.rewrite {
      case e:Entity => {
        val found = returnItems.find(_.columnName == e.entityName)
        
        found match {
          case None => e
          case Some(returnItem) => returnItem.expression
        }

      }
      case somethingElse => somethingElse
    }

    SortItem(expression, in.ascending)
  }
}