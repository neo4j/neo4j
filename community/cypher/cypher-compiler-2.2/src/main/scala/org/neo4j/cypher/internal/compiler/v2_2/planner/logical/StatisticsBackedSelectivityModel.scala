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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, TokenContext}

class StatisticsBackedSelectivityModel(stats: GraphStatistics, queryGraph: QueryGraph, tokenLookups: TokenContext)
  extends Metrics.SelectivityModel {
  def apply(e: Expression): Selectivity = e match {
      case In(Property(Identifier(name), PropertyKeyName(propertyKeyName)), Collection(expressions)) if isANode(name) =>
        val labels = queryGraph.selections.labelsOnNode(IdName(name)).toSeq

        for (label <- labels) yield {
          val maybeLabelId = tokenLookups.getOptLabelId(label.name)
          val maybePropId = tokenLookups.getOptPropertyKeyId(propertyKeyName)

          (maybeLabelId, maybePropId) match {
            case (Some(labelId), Some(propId)) =>
              val indexSelectivity = stats.indexSelectivity(LabelId(labelId), PropertyKeyId(propId))
              (indexSelectivity.inverse ^ expressions.length).inverse // Multiple value in expressions are treated as if they were OR:ed together

            case _ => Selectivity(0) // If the label or property name are unknown, we'll find no matches
          }
        }

      case Not(inner) =>
        apply(inner).inverse

      case Ors(innerExpressions) =>
        val chanceOfNotMatching: Selectivity = innerExpressions.toSeq.map(this.apply).map(_.inverse)
        chanceOfNotMatching.inverse

      case x =>
        ???
    }

  def isANode(name: String) = queryGraph.patternNodes.contains(IdName(name))
}
