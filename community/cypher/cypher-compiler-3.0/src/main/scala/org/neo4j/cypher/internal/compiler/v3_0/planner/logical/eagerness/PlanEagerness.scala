/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, LogicalPlanningFunction3}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{PlannerQuery, QueryGraph}

case class PlanEagerness(planUpdates: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan])
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val containsMerge = plannerQuery.queryGraph.containsMergeRecursive

    //--------------
    // Plan eagerness before updates (on lhs)
    // This is needed to protect the reads from updates from the same QG.
    // (I) Protect reads from future writes
    val (thisReadOverlapsWrites, conflictIsInThisQG) = readOverlapsFutureWrites(plannerQuery, lhs, head, containsMerge)

    // NOTE: In case this is a merge, eagerness has to be planned on top of updates, and not on lhs (see (III) below)
    val newLhs = if (thisReadOverlapsWrites && !containsMerge && conflictIsInThisQG)
      context.logicalPlanProducer.planEager(lhs)
    else
      lhs

    //--------------
    // Plan updates
    val updatePlan = planUpdates(plannerQuery, newLhs, head)

    //--------------
    // Plan eagerness after updates
    // (II) Protect writes from future reads
    val thisWriteOverlapsFutureReads = writeOverlapsFutureReads(plannerQuery, head)
    // (III) Protect merge reads from future writes, or normal reads against updates down the line
    val thisReadOverlapsFutureWrites = (containsMerge || !conflictIsInThisQG) && thisReadOverlapsWrites

    if (thisWriteOverlapsFutureReads || thisReadOverlapsFutureWrites)
      context.logicalPlanProducer.planEager(updatePlan)
    else
      updatePlan
  }

  private def readOverlapsFutureWrites(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean,
                                       containsMerge: Boolean): (Boolean, Boolean) = {

    val thisRead: Read = readQGWithoutStableNodeVariable(plannerQuery, lhs, head)
    val allUpdates = plannerQuery.allQueryGraphs.map(_.updates)

    // Merge needs to see it's own changes, so if the conflict is only between the reads and writes of MERGE,
    // we should not be eager
    val futureUpdates = if (containsMerge)
      allUpdates.tail
    else
      allUpdates

    val (overlaps, onFirstWrite) = futureUpdates.collectFirst {
      case update if update overlaps thisRead => (true, update == plannerQuery.queryGraph.updates)
    }.getOrElse(false -> false)

    (overlaps, onFirstWrite)
  }

  private def writeOverlapsFutureReads(plannerQuery: PlannerQuery, head: Boolean): Boolean = {
    val queryGraph = plannerQuery.queryGraph
    if (head && queryGraph.writeOnly)
      false // No conflict if the first query graph only contains writes
    else {
      val thisWrite = queryGraph.updates
      val futureReads = plannerQuery.allQueryGraphs.tail.map(_.reads)

      val knownFutureConflict = futureReads.exists(thisWrite.overlaps)
      val deleteAndLaterMerge = queryGraph.containsDeleteRecursive && plannerQuery.allQueryGraphs.exists(_.containsMergeRecursive)
      knownFutureConflict || deleteAndLaterMerge
    }
  }

  private def readQGWithoutStableNodeVariable(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean): Read = {
    val originalQG = plannerQuery.queryGraph
    val graph = lhs.leftMost match {
      case n: NodeLogicalLeafPlan if head && includesAllPredicates(originalQG, n)  => originalQG.withoutPatternNode(n.idName)
      case _ => originalQG
    }
    graph.reads
  }

  def includesAllPredicates(originalQG: QueryGraph, n: NodeLogicalLeafPlan): Boolean = {
    def p(qg: QueryGraph) = qg.selections.predicatesGiven(qg.argumentIds + n.idName).toSet

    val a = p(n.solved.lastQueryGraph)
    val b = p(originalQG)
    a == b
  }
}
