/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.plannerQuery.PlannerQueryBuilder
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.frontend.v3_2.phases.Condition
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.ir.v3_2._

import scala.collection.mutable

object OptionalMatchReorganizer extends PlannerQueryRewriter {

  override def instance(context: CompilerContext): Rewriter = OptionalMatchReorganizer._instance

  override def description: String = "Reorganizes optional matches in a QG to make them easy to compare"

  override def postConditions: Set[Condition] = Set.empty

  private val _instance: Rewriter = bottomUp(Rewriter.lift {
    case originalPQ@RegularPlannerQuery(qg, originalHorizon, originalTail) if qg.optionalMatches.size > 1 => qg
      val clicks = mutable.Set[Seq[QueryGraph]]()
      val remaining = mutable.ListBuffer(qg.optionalMatches:_*)
      while(remaining.nonEmpty) {
        val current = remaining.remove(0)
        val dependentOptionalMatches = remaining.filter(qg => {
          val overlaps = qg.dependencies intersect current.allCoveredIds
          (overlaps -- qg.argumentIds).nonEmpty
        })

        remaining --= dependentOptionalMatches
        clicks += current +: dependentOptionalMatches
      }

      if (clicks.size == 1) {
        originalPQ
      } else {
        val newQg = cleanUpArgumentIds(qg.coveredIds, qg.copy(optionalMatches = clicks.head.toIndexedSeq))
        val newTails = clicks.tail.foldRight[Option[PlannerQuery]](originalTail) {
          case (optionalMatches, tailAcc) =>
            val qg = QueryGraph.empty.withOptionalMatches(optionalMatches.toIndexedSeq)
            val horizon =
              if (tailAcc == originalTail) // The very last PQ should contain the original horizon, so we do not loose that
                originalHorizon
              else
                PassthroughAllHorizon() // In all other cases, we just want data to flow through this horizon
            Some(RegularPlannerQuery(qg, horizon, tailAcc))

        }


        val newTail = newTails.get

        def fixup(incoming: Set[IdName], pq: PlannerQuery): PlannerQuery = {
          val newPq = pq.withQueryGraph(cleanUpArgumentIds(incoming, pq.queryGraph))
          if(newPq.tail == originalTail)
            newPq
          else {
            val exposedVariables = newPq.horizon.exposedSymbols(newPq.queryGraph.coveredIds)
            newPq.updateTail(fixup(exposedVariables, _))
          }
        }

        val rewrittenPQ = originalPQ.withHorizon(PassthroughAllHorizon()).withQueryGraph(newQg).replaceTail(newTail)
        fixup(originalPQ.queryGraph.argumentIds, rewrittenPQ)
      }
  })

  private def cleanUpArgumentIds(incomingArgs: Set[IdName], queryGraph: QueryGraph) = {
    PlannerQueryBuilder.setArgumentIdsOnOptionalMatches(queryGraph.withArgumentIds(incomingArgs))
  }
}