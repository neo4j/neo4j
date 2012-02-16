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

object OrderByRewriter {
  def apply(q: Query): Query = {
    if (q.sort.isEmpty)
      q
    else {
      val a = q.aggregation match {
        case Some(agg) => agg.aggregationItems
        case None => Seq()
      }

      val s = q.sort.get.sortItems
      val r = q.returns.returnItems

      val newSort = s.map(si => {
        val agg = a.find(ai => si.returnItem.equalsWithoutName(ai))
        val ret = r.find(ri => si.returnItem.equalsWithoutName(ri))

        cleanUpSortItem((si, agg, ret))
      })

      Query(q.returns, q.start, q.matching, q.where, q.aggregation, Some(Sort(newSort:_*)), q.slice, q.namedPaths, q.having, q.queryString)
    }
  }

  private def cleanUpSortItem(x: (SortItem, Option[ReturnItem], Option[ReturnItem])): SortItem = x match {
    case (si, Some(a), _) => SortItem(a, si.ascending)
    case (si, _, Some(r)) => SortItem(r, si.ascending)
    case (si, _, _) => si
  }
}