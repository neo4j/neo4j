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

      val s = q.sort match {
        case Some(sort) => sort.sortItems
      }

      val r = q.returns.returnItems

      val newSort = s.map(si => {
        val agg = a.find(ai => si.returnItem.equalsWithoutName(ai))
        val ret = r.find(ri => si.returnItem.equalsWithoutName(ri))

        cleanUpSortItem((si, agg, ret))
      })

      Query(q.returns, q.start, q.matching, q.where, q.aggregation, Some(Sort(newSort:_*)), q.slice, q.namedPaths, q.queryString)
    }
  }

  private def cleanUpSortItem(x: (SortItem, Option[AggregationItem], Option[ReturnItem])): SortItem = x match {
    case (si, Some(a), _) => SortItem(a, si.ascending)
    case (si, _, Some(r)) => SortItem(r, si.ascending)
    case (si, _, _) => si
  }
}