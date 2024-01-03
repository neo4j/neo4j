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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren

import scala.annotation.tailrec

object PropertyAccessHelper {

  case class PropertyAccess(variable: LogicalVariable, propertyName: String)

  /*
   * Find all direct property accesses in the head of this planner query. Used when selecting between index plans.
   */
  def findLocalPropertyAccesses(query: SinglePlannerQuery): Set[PropertyAccess] = {
    val propertyAccessLocations: Seq[Any] = Seq(
      query.horizon,
      query.queryGraph,
      query.interestingOrder
    )

    propertyAccessLocations.folder.treeFold(Set[PropertyAccess]()) {
      case Property(v: Variable, PropertyKeyName(propName)) => set =>
          SkipChildren(set + PropertyAccess(v, propName))
    }
  }

  /*
   * Find all properties over which aggregation is performed, where we potentially could use a NodeIndexScan.
   */
  def findAggregationPropertyAccesses(query: SinglePlannerQuery): Set[PropertyAccess] = {

    // The renamings map is used to keep track of any projections changing the name of the property,
    // as in MATCH (n:Label) WITH n.prop1 AS prop RETURN count(prop)
    @tailrec
    def rec(currentQuery: SinglePlannerQuery, renamings: Map[LogicalVariable, Expression]): Set[PropertyAccess] = {
      // If the graph is mutated between the MATCH and the aggregation, an index scan might lead to the wrong number of mutations
      if (currentQuery.queryGraph.mutatingPatterns.nonEmpty) return Set.empty

      currentQuery.horizon match {
        case aggr: AggregatingQueryProjection =>
          // needed here to not enter next case
          if (aggr.groupingExpressions.isEmpty)
            AggregationHelper.extractProperties(aggr.aggregationExpressions, renamings)
          else Set.empty

        case proj: QueryProjection =>
          currentQuery.tail match {
            case Some(tail) => rec(tail, renamings ++ proj.projections)
            case _          => Set.empty
          }

        case _ =>
          currentQuery.tail match {
            case Some(tail) => rec(tail, renamings)
            case _          => Set.empty
          }
      }
    }

    rec(query, Map.empty)
  }

}
