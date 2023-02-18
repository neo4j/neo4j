/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

case object bfsAggregationRemover extends Rewriter {

  private case class DistinctHorizon(
    aggregatingPlan: AggregatingPlan,
    hasCardinalityIncrease: Boolean,
    bfsPruningVarExpand: Option[BFSPruningVarExpand]
  ) {

    /**
     * Aggregating plan is a [[Aggregation]] || [[OrderedAggregation]]
     *
     * @return true, iff the plan can not be rewritten to a [[Projection]], but some aggregating functions can be modified to not be distinct.
     */
    def isRelaxableAggregation: Boolean = bfsPruningVarExpand match {
      case Some(bfs) =>
        !hasCardinalityIncrease &&
        aggregatingPlan.aggregationExpressions.nonEmpty &&
        (
          aggregatingPlan.groupingExpressions.isEmpty ||
            aggregatingPlan.groupingExpressions.values.exists {
              case variable: Variable => bfs.from == variable.name
              case _                  => false
            }
        ) &&
        aggregatingPlan.aggregationExpressions.values.exists(DistinctHorizon.isDistinct(_, bfs.to))
      case None =>
        false
    }

    /**
     * Aggregating plan is a [[Aggregation]] || [[OrderedAggregation]]
     *
     * @return true, iff the plan can be rewritten to a [[Projection]].
     */
    def isRemovableAggregation: Boolean = bfsPruningVarExpand match {
      case Some(bfs) =>
        !hasCardinalityIncrease &&
        aggregatingPlan.aggregationExpressions.nonEmpty &&
        groupingsAreSafeToRemove(bfs) &&
        aggregationsAreSafeToRemove(bfs)
      case None =>
        false
    }

    /**
     * Aggregating plan is a [[Distinct]] || [[OrderedDistinct]]
     *
     * @return true, iff the plan can be rewritten to a [[Projection]].
     */
    def isRemovableDistinct: Boolean = bfsPruningVarExpand match {
      case Some(bfs) =>
        !hasCardinalityIncrease &&
        aggregatingPlan.aggregationExpressions.isEmpty &&
        groupingsAreSafeToRemove(bfs)
      case None =>
        false
    }

    def convertAggregationExpressionToProjectionExpressions: Map[String, Expression] = {
      aggregatingPlan.aggregationExpressions.map {
        case key -> FunctionInvocation(
            _,
            FunctionName("min"),
            _,
            Seq(variable: Variable)
          ) =>
          key -> variable
        case e =>
          throw new IllegalStateException(
            s"Unexpectedly encountered an aggregation expression that is not min(depth): $e"
          )
      } ++ aggregatingPlan.groupingExpressions
    }

    def relaxAggregationExpressions: Map[String, Expression] = {
      aggregatingPlan.aggregationExpressions.map {
        case (
            key,
            fun @ FunctionInvocation(
              _,
              _,
              true,
              Seq(variable: Variable)
            )
          ) if variable.name == bfsPruningVarExpand.get.to =>
          key -> fun.copy(distinct = false)(fun.position)
        case k -> v =>
          k -> v
      }
    }

    /**
     * Valid distinct columns combinations, i.e., combinations that allow the Distinct to be removed:
     * * bfs.to
     * * bfs.from, bfs.to
     *
     * @return true, iff it safe to rewrite the grouping expressions of this [[AggregatingPlan]] into a [[Projection]]
     */
    private def groupingsAreSafeToRemove(bfs: BFSPruningVarExpand): Boolean = {
      val allDistinctColumnsAreBfsNodeVariables = aggregatingPlan.groupingExpressions.values.forall(e =>
        e match {
          case Variable(name) => name == bfs.from || name == bfs.to
          case _              => false
        }
      )
      val distinctKeys = aggregatingPlan.groupingExpressions.values.flatMap(_.dependencies.map(_.name)).toSet
      val isToOnly = distinctKeys.size == 1 && (distinctKeys - bfs.to).isEmpty
      val isFromAndTo = distinctKeys.size == 2 && (distinctKeys - bfs.from - bfs.to).isEmpty
      aggregatingPlan.groupingExpressions.nonEmpty &&
      allDistinctColumnsAreBfsNodeVariables &&
      (isToOnly || isFromAndTo)
    }

    /**
     * An [[Aggregation]] is considered safe to rewrite to [[Projection]] iff all aggregating functions are min(bfs.depth)
     *
     * @return true, iff it safe to rewrite the grouping expressions of this [[AggregatingPlan]] into a [[Projection]]
     */
    private def aggregationsAreSafeToRemove(bfs: BFSPruningVarExpand): Boolean = {
      bfs.depthName match {
        case Some(depthName) =>
          aggregatingPlan.aggregationExpressions.values.forall(DistinctHorizon.isMin(_, depthName))
        case None =>
          false
      }
    }
  }

  private object DistinctHorizon {

    val empty: DistinctHorizon =
      DistinctHorizon(aggregatingPlan = null, hasCardinalityIncrease = true, bfsPruningVarExpand = None)

    def isDistinct(e: Expression, name: String = null): Boolean = e match {
      case FunctionInvocation(
          _,
          _,
          true,
          Seq(variable: Variable)
        ) => name == null || name == variable.name
      case _ =>
        false
    }

    def isMin(e: Expression, name: String = null): Boolean = e match {
      case FunctionInvocation(
          _,
          FunctionName("min"),
          _,
          Seq(variable: Variable)
        ) => name == null || name == variable.name
      case _ =>
        false
    }
  }

  private case class ReplacementPlans(
    aggregatingPlansToRemove: Map[AggregatingPlan, Map[String, Expression]],
    distinctsToRemove: Map[AggregatingPlan, Map[String, Expression]],
    aggregatingPlansToRelax: Map[AggregatingPlan, Map[String, Expression]]
  )

  private def findReplacementPlans(plan: LogicalPlan): ReplacementPlans = {
    val aggregatingPlansToRemove: mutable.Map[AggregatingPlan, Map[String, Expression]] = mutable.Map.empty
    val distinctsToRemove: mutable.Map[AggregatingPlan, Map[String, Expression]] = mutable.Map.empty
    val aggregatingPlansToRelax: mutable.Map[AggregatingPlan, Map[String, Expression]] = mutable.Map.empty

    def collectDistinctSet(plan: LogicalPlan, distinctHorizon: DistinctHorizon): DistinctHorizon = {
      plan match {
        case aggPlan: AggregatingPlan
          if aggPlan.aggregationExpressions.values.forall(e =>
            DistinctHorizon.isMin(e) || DistinctHorizon.isDistinct(e)
          ) =>
          DistinctHorizon(aggPlan, hasCardinalityIncrease = false, bfsPruningVarExpand = None)

        case bfs: BFSPruningVarExpand if !distinctHorizon.hasCardinalityIncrease =>
          distinctHorizon.bfsPruningVarExpand match {
            case Some(_) => distinctHorizon.copy(hasCardinalityIncrease = true)
            case None    => distinctHorizon.copy(bfsPruningVarExpand = Some(bfs))
          }

        case _: Projection |
          _: Selection |
          _: Eager |
          _: CacheProperties =>
          distinctHorizon

        case _: Argument |
          _: DirectedRelationshipUniqueIndexSeek |
          _: UndirectedRelationshipUniqueIndexSeek |
          _: NodeUniqueIndexSeek =>
          if (distinctHorizon.isRemovableDistinct) {
            distinctsToRemove.put(
              distinctHorizon.aggregatingPlan,
              distinctHorizon.aggregatingPlan.groupingExpressions
            )
          } else if (distinctHorizon.isRemovableAggregation) {
            aggregatingPlansToRemove.put(
              distinctHorizon.aggregatingPlan,
              distinctHorizon.convertAggregationExpressionToProjectionExpressions
            )
          } else if (distinctHorizon.isRelaxableAggregation) {
            aggregatingPlansToRelax.put(
              distinctHorizon.aggregatingPlan,
              distinctHorizon.relaxAggregationExpressions
            )
          }
          DistinctHorizon.empty

        case _ =>
          DistinctHorizon.empty
      }
    }

    val planStack = new mutable.Stack[(LogicalPlan, DistinctHorizon)]()
    planStack.push((plan, DistinctHorizon.empty))

    while (planStack.nonEmpty) {
      val (plan: LogicalPlan, distinctHorizon: DistinctHorizon) = planStack.pop()
      val newDistinctHorizon = collectDistinctSet(plan, distinctHorizon)

      plan.lhs.foreach(p => planStack.push((p, newDistinctHorizon)))
      plan.rhs.foreach(p => planStack.push((p, newDistinctHorizon)))
    }

    ReplacementPlans(aggregatingPlansToRemove.toMap, distinctsToRemove.toMap, aggregatingPlansToRelax.toMap)
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val replacementPlans = findReplacementPlans(plan)

        val innerRewriter = topDown(Rewriter.lift {

          case aggregation: Aggregation if replacementPlans.aggregatingPlansToRemove.contains(aggregation) =>
            Projection(
              aggregation.source,
              discardSymbols = Set.empty,
              projectExpressions = replacementPlans.aggregatingPlansToRemove(aggregation)
            )(SameId(aggregation.id))

          case aggregation: Aggregation if replacementPlans.aggregatingPlansToRelax.contains(aggregation) =>
            aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlansToRelax(aggregation))(
              SameId(aggregation.id)
            )

          case aggregation: OrderedAggregation if replacementPlans.aggregatingPlansToRemove.contains(aggregation) =>
            Projection(
              aggregation.source,
              discardSymbols = Set.empty,
              projectExpressions = replacementPlans.aggregatingPlansToRemove(aggregation)
            )(SameId(aggregation.id))

          case aggregation: OrderedAggregation if replacementPlans.aggregatingPlansToRelax.contains(aggregation) =>
            aggregation.copy(aggregationExpressions = replacementPlans.aggregatingPlansToRelax(aggregation))(
              SameId(aggregation.id)
            )

          case distinct: Distinct if replacementPlans.distinctsToRemove.contains(distinct) =>
            Projection(
              distinct.source,
              discardSymbols = Set.empty,
              projectExpressions = distinct.groupingExpressions
            )(SameId(distinct.id))

          case distinct: OrderedDistinct if replacementPlans.distinctsToRemove.contains(distinct) =>
            Projection(
              distinct.source,
              discardSymbols = Set.empty,
              projectExpressions = distinct.groupingExpressions
            )(SameId(distinct.id))
        })
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}
