/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{QueryPlannerKit, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan

import scala.annotation.tailrec

case object connectComponentsAndSolveOptionalMatch {
  def apply(plans: Set[LogicalPlan], qg: QueryGraph)
           (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {

    def findBestCartesianProduct(plans: Set[LogicalPlan]): Set[LogicalPlan] = {

      assert(plans.size > 1, "Can't build cartesian product with less than two input plans")

      val allCrossProducts = (for (p1 <- plans; p2 <- plans if p1 != p2) yield {
        val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(p1, p2), qg)
        (crossProduct, (p1, p2))
      }).toMap
      val bestCartesian = kit.pickBest(allCrossProducts.keySet).get
      val (p1, p2) = allCrossProducts(bestCartesian)

      plans - p1 - p2 + bestCartesian
    }

    @tailrec
    def recurse(plans: Set[LogicalPlan], optionalMatches: Seq[QueryGraph]): (Set[LogicalPlan], Seq[QueryGraph]) = {
      if (optionalMatches.nonEmpty) {
        // If we have optional matches left to solve - start with that
        val firstOptionalMatch = optionalMatches.head
        val applicablePlan = plans.find(p => firstOptionalMatch.argumentIds subsetOf p.availableSymbols)

        applicablePlan match {
          case Some(p) =>
            val candidates = context.config.optionalSolvers.flatMap(solver => solver(firstOptionalMatch, p))
            val best = kit.pickBest(candidates).get
            recurse(plans - p + best, optionalMatches.tail)

          case None =>
            // If we couldn't find any optional match we can take on, produce the best cartesian product possible
            recurse(findBestCartesianProduct(plans), optionalMatches)
        }
      }
      else if (plans.size > 1) {
        recurse(findBestCartesianProduct(plans), optionalMatches)
      }
      else (plans, optionalMatches)
    }

    val (resultingPlans, optionalMatches) = recurse(plans, qg.optionalMatches)
    assert(resultingPlans.size == 1)
    assert(optionalMatches.isEmpty)
    resultingPlans.head
  }
}
