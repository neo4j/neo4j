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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{IdName, LogicalPlan}

case class CandidateList(plans: Seq[LogicalPlan] = Seq.empty) {
  def pruned: CandidateList = {
    def overlap(a: Set[IdName], b: Set[IdName]) = !a.intersect(b).isEmpty

    val (_, result: Seq[LogicalPlan]) = plans.foldLeft(Set.empty[IdName] -> Seq.empty[LogicalPlan]) {
      case ((covered, partial), plan) =>
        if (overlap(covered, plan.coveredIds))
          (covered, partial)
        else
          (covered ++ plan.coveredIds, partial :+ plan)
    }
    CandidateList(result)
  }

  def sorted = CandidateList(plans.sortBy(c => (c.cost, -c.coveredIds.size)))

  def ++(other: CandidateList): CandidateList = CandidateList(plans ++ other.plans)

  def +(plan: LogicalPlan) = copy(plans :+ plan)

  def topPlan = sorted.pruned.plans.headOption

  def map(f: LogicalPlan => LogicalPlan):CandidateList = copy(plans = plans.map(f))
}
