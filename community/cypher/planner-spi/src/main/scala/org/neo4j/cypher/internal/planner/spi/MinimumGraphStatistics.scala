/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.v4_0.util.{Cardinality, LabelId, RelTypeId, Selectivity}

object MinimumGraphStatistics {
  val MIN_NODES_ALL: Int = 10
  val MIN_NODES_WITH_LABEL: Int = 10
  val MIN_PATTERN_STEP: Int = 1

  val MIN_NODES_ALL_CARDINALITY = Cardinality(MIN_NODES_ALL)
  val MIN_NODES_WITH_LABEL_CARDINALITY = Cardinality(MIN_NODES_WITH_LABEL)
  val MIN_PATTERN_STEP_CARDINALITY = Cardinality(MIN_PATTERN_STEP)
}

/**
  * The relative costs of the different operators are estimated for graphs of some size, because no database user
  * will have graphs with <10 nodes. This means thought that applying these costs to single-digit graphs gives surprising
  * results, and plans that will be clearly suboptimal for larger graphs. This is mostly fine, as we force replanning
  * when the graph grows, but for large import queries this could have drastic performance impact in case the initial
  * import query gets stuck with a plan that becomes unusable towards the end of query invocation.
  */
class MinimumGraphStatistics(delegate: GraphStatistics) extends DelegatingGraphStatistics(delegate) {

  import MinimumGraphStatistics._

  override def nodesAllCardinality(): Cardinality =
    atLeast(delegate.nodesAllCardinality().amount, MIN_NODES_ALL_CARDINALITY)

  override def nodesWithLabelCardinality(maybeLabelId: Option[LabelId]): Cardinality = {
    atLeast(delegate.nodesWithLabelCardinality(maybeLabelId), MIN_NODES_WITH_LABEL_CARDINALITY)
  }

  override def patternStepCardinality(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
    val emulatedCompleteCardinality =
      (fromLabel, toLabel) match {
        case (Some(_), Some(_)) =>
          // Keeping transactionally updated (:A)-[:R]->(:B) statistics is prohibitively expensive and we haven't
          // implemented that yet. As a best effort work around, we use the smallest of (:A)-[:R]->() and ()-[:R]->(:B),
          // upper bounds the count we're after.
          Cardinality.min(
            delegate.patternStepCardinality(fromLabel, relTypeId, None),
            delegate.patternStepCardinality(None, relTypeId, toLabel)
          )
        case _ =>
          delegate.patternStepCardinality(fromLabel, relTypeId, toLabel)
      }
    atLeast(emulatedCompleteCardinality, MIN_PATTERN_STEP_CARDINALITY)
  }

  private def atLeast(x: Cardinality, minimum: Cardinality): Cardinality = {
    if (x.amount < minimum.amount)
      minimum
    else
      x
  }
}
