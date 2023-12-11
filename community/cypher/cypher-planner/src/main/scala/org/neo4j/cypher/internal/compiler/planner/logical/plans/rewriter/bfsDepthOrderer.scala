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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Removes [[Sort]] plans (or relaxes them to [[PartialSort]]) that are no longer necessary after rewriting [[VarExpand]] into [[BFSPruningVarExpand]].
 */
case object bfsDepthOrderer extends Rewriter {

  private case class SortHorizon(
    sortPlan: Option[LogicalPlan],
    lastCardinalityIncreasingPlan: Option[LogicalPlan],
    dependencies: Set[String]
  )

  private object SortHorizon {

    val empty: SortHorizon =
      SortHorizon(sortPlan = None, lastCardinalityIncreasingPlan = None, dependencies = Set.empty)
  }

  private case class ReplacementPlans(makePartialSorts: Map[LogicalPlan, String], removeSorts: Map[LogicalPlan, String])

  private def findReplacementPlans(plan: LogicalPlan): ReplacementPlans = {
    val makePartialSorts: mutable.Map[LogicalPlan, String] = mutable.Map.empty
    val removeSorts: mutable.Map[LogicalPlan, String] = mutable.Map.empty

    def recordSortPlanForRewriting(sortHorizon: SortHorizon, depthName: String): Unit = {
      sortHorizon.sortPlan match {
        case Some(sort @ Sort(_, Seq(Ascending(_)))) if sortHorizon.dependencies.contains(depthName) =>
          removeSorts.put(sort, depthName)

        case Some(sort @ Sort(_, Ascending(_) +: _)) if sortHorizon.dependencies.contains(depthName) =>
          makePartialSorts.put(sort, depthName)

        case Some(top @ Top(_, Seq(Ascending(_)), _)) if sortHorizon.dependencies.contains(depthName) =>
          removeSorts.put(top, depthName)

        case Some(top @ Top(_, Ascending(_) +: _, _)) if sortHorizon.dependencies.contains(depthName) =>
          makePartialSorts.put(top, depthName)

        case _ => // do nothing
      }
    }

    def collectSortPlans(plan: LogicalPlan, sortHorizon: SortHorizon): SortHorizon = {
      plan match {
        case sort: Sort =>
          SortHorizon(Some(sort), lastCardinalityIncreasingPlan = None, dependencies = Set(sort.sortItems.head.id.name))

        case top: Top =>
          SortHorizon(Some(top), lastCardinalityIncreasingPlan = None, dependencies = Set(top.sortItems.head.id.name))

        case _: BFSPruningVarExpand |
          _: PruningVarExpand |
          _: VarExpand |
          _: Expand |
          _: UnwindCollection if sortHorizon.sortPlan.nonEmpty =>
          sortHorizon.copy(lastCardinalityIncreasingPlan = Some(plan))

        case projection: Projection if sortHorizon.sortPlan.nonEmpty =>
          val aliases = mutable.Set.empty[String]
          projection.projectExpressions.foreach {
            case (key, Variable(name)) if sortHorizon.dependencies.contains(key.name) => aliases += name
            case _                                                                    => // do nothing
          }
          sortHorizon.copy(dependencies = sortHorizon.dependencies ++ aliases)

        case _: Selection |
          _: Eager |
          _: Optional |
          _: CacheProperties =>
          sortHorizon

        case _: Argument |
          _: DirectedRelationshipUniqueIndexSeek |
          _: UndirectedRelationshipUniqueIndexSeek |
          _: NodeUniqueIndexSeek =>
          sortHorizon.lastCardinalityIncreasingPlan match {
            case Some(BFSPruningVarExpand(_, _, _, _, _, _, _, Some(depthName), _, _, _)) =>
              recordSortPlanForRewriting(sortHorizon, depthName.name)
            case _ => // do nothing
          }

          SortHorizon.empty

        case _ =>
          SortHorizon.empty
      }
    }

    val planStack = new mutable.Stack[(LogicalPlan, SortHorizon)]()
    planStack.push((plan, SortHorizon.empty))

    while (planStack.nonEmpty) {
      val (plan: LogicalPlan, distinctHorizon: SortHorizon) = planStack.pop()
      val newSortHorizon = collectSortPlans(plan, distinctHorizon)

      plan.lhs.foreach(p => planStack.push((p, newSortHorizon)))
      plan.rhs.foreach(p => planStack.push((p, newSortHorizon)))
    }

    ReplacementPlans(makePartialSorts.toMap, removeSorts.toMap)
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val replacementPlans = findReplacementPlans(plan)
        val makePartialSorts = replacementPlans.makePartialSorts
        val removeSorts = replacementPlans.removeSorts

        val innerRewriter = topDown(Rewriter.lift {
          case sort: Sort if makePartialSorts.contains(sort) =>
            PartialSort(sort.source, Seq(sort.sortItems.head), sort.sortItems.tail)(SameId(sort.id))

          case top: Top if makePartialSorts.contains(top) =>
            PartialTop(top.source, Seq(top.sortItems.head), top.sortItems.tail, top.limit)(SameId(top.id))

          case sort: Sort if removeSorts.contains(sort) =>
            sort.source

          case top: Top if removeSorts.contains(top) =>
            Limit(top.source, top.limit)(SameId(top.id))
        })
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}
