/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Metrics.CostModel

case class CandidateList(plans: Seq[QueryPlan] = Seq.empty) {

  def ++(other: CandidateList): CandidateList = CandidateList(plans ++ other.plans)

  def +(plan: QueryPlan) = copy(plans :+ plan)

  def bestPlan(costs: CostModel): Option[QueryPlan] = {
    val sortedPlans = plans.sortBy[(Int, Cost, Int)](c => (-c.solved.numHints, costs(c.plan), -c.availableSymbols.size))
    sortedPlans.headOption
  }

  def map(f: QueryPlan => QueryPlan): CandidateList = copy(plans = plans.map(f))

  def isEmpty = plans.isEmpty
}

object Candidates {
  def apply(plans: QueryPlan*): CandidateList = CandidateList(plans)
}
