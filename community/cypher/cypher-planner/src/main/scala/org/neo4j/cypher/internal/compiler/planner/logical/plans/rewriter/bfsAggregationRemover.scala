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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Min
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
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Removes [[Distinct]] and [[Aggregation]] plans that are no longer necessary after rewriting [[org.neo4j.cypher.internal.logical.plans.VarExpand]] into [[BFSPruningVarExpand]].
 */
case object bfsAggregationRemover extends Rewriter {

  private case class DistinctHorizon(
    aggregatingPlan: AggregatingPlan,
    hasCardinalityIncrease: Boolean,
    bfsPruningVarExpand: BFSPruningVarExpand,
    groupingDependencies: Set[String]
  ) {

    /**
     * Aggregating plan is a [[Aggregation]] || [[OrderedAggregation]]
     *
     * @return true, iff the plan can not be rewritten to a [[Projection]], but some aggregating functions can be modified to not be distinct.
     */
    def isRelaxableAggregation: Boolean = {
      bfsPruningVarExpand != null &&
      !hasCardinalityIncrease &&
      aggregatingPlan.aggregationExpressions.nonEmpty &&
      (aggregatingPlan.groupingExpressions.isEmpty || groupingDependencies.contains(bfsPruningVarExpand.from.name)) &&
      aggregatingPlan.aggregationExpressions.values.exists(DistinctHorizon.isDistinct(_, bfsPruningVarExpand.to.name))
    }

    /**
     * Aggregating plan is a [[Aggregation]] || [[OrderedAggregation]]
     *
     * @return true, iff the plan can be rewritten to a [[Projection]].
     */
    def isRemovableAggregation: Boolean = {
      bfsPruningVarExpand != null &&
      !hasCardinalityIncrease &&
      aggregatingPlan.aggregationExpressions.nonEmpty &&
      groupingsAreSafeToRemove(bfsPruningVarExpand) &&
      aggregationsAreSafeToRemove(bfsPruningVarExpand)
    }

    /**
     * Aggregating plan is a [[Distinct]] || [[OrderedDistinct]]
     *
     * @return true, iff the plan can be rewritten to a [[Projection]].
     */
    def isRemovableDistinct: Boolean = {
      bfsPruningVarExpand != null &&
      !hasCardinalityIncrease &&
      aggregatingPlan.aggregationExpressions.isEmpty &&
      groupingsAreSafeToRemove(bfsPruningVarExpand)
    }

    def convertAggregationExpressionToProjectionExpressions: Map[String, Expression] = {
      aggregatingPlan.aggregationExpressions.map {
        case key -> Min(variable: Variable) => key.name -> variable
        case e =>
          throw new IllegalStateException(
            s"Unexpectedly encountered an aggregation expression that is not min(depth): $e"
          )
      } ++ aggregatingPlan.groupingExpressions.map { case (key, value) => key.name -> value }
    }

    def relaxAggregationExpressions: Map[String, Expression] = {
      aggregatingPlan.aggregationExpressions.map {
        case (key, fun @ FunctionInvocation(_, _, true, Seq(variable: Variable), _, _))
          if variable.name == bfsPruningVarExpand.to.name =>
          key.name -> fun.copy(distinct = false)(fun.position)
        case k -> v =>
          k.name -> v
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
      val isToOnly = aggregatingPlan.groupingExpressions.size == 1 &&
        groupingDependencies.contains(bfs.to.name)
      val isFromAndToOnly = aggregatingPlan.groupingExpressions.size == 2 &&
        groupingDependencies.contains(bfs.from.name) && groupingDependencies.contains(bfs.to.name)
      aggregatingPlan.groupingExpressions.nonEmpty && (isToOnly || isFromAndToOnly)
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
      DistinctHorizon(
        aggregatingPlan = null,
        hasCardinalityIncrease = true,
        bfsPruningVarExpand = null,
        groupingDependencies = Set.empty
      )

    def isDistinct(e: Expression, name: String = null): Boolean = e match {
      case FunctionInvocation(_, _, true, Seq(variable: Variable), _, _) =>
        name == null || name == variable.name
      case _ =>
        false
    }

    def isMin(e: Expression, name: LogicalVariable = null): Boolean = e match {
      case Min(variable: Variable) => name == null || name.name == variable.name
      case _                       => false
    }
  }

  private case class ReplacementPlans(
    aggregatingPlansToRemove: Map[AggregatingPlan, Map[String, Expression]],
    aggregatingPlansToRelax: Map[AggregatingPlan, Map[String, Expression]]
  )

  private def findReplacementPlans(plan: LogicalPlan): ReplacementPlans = {
    val aggregatingPlansToRemove: mutable.Map[AggregatingPlan, Map[String, Expression]] = mutable.Map.empty
    val aggregatingPlansToRelax: mutable.Map[AggregatingPlan, Map[String, Expression]] = mutable.Map.empty

    def collectDistinctSet(plan: LogicalPlan, distinctHorizon: DistinctHorizon): DistinctHorizon = {
      plan match {
        case aggPlan: AggregatingPlan
          if aggPlan.aggregationExpressions.values.forall(e =>
            DistinctHorizon.isMin(e) || DistinctHorizon.isDistinct(e)
          ) =>
          val groupingDependencies = aggPlan.groupingExpressions.values.flatMap(_.dependencies.map(_.name)).toSet
          DistinctHorizon(aggPlan, hasCardinalityIncrease = false, bfsPruningVarExpand = null, groupingDependencies)

        case bfs: BFSPruningVarExpand if !distinctHorizon.hasCardinalityIncrease =>
          if (distinctHorizon.bfsPruningVarExpand != null) {
            distinctHorizon.copy(hasCardinalityIncrease = true)
          } else {
            distinctHorizon.copy(bfsPruningVarExpand = bfs)
          }

        case projection: Projection if !distinctHorizon.hasCardinalityIncrease =>
          val newGroupingDependencies = distinctHorizon.groupingDependencies ++ projection.projectExpressions.collect {
            case (key, Variable(name)) if distinctHorizon.groupingDependencies.contains(key.name) => name
          }
          distinctHorizon.copy(groupingDependencies = newGroupingDependencies)

        case _: Selection |
          _: Eager |
          _: CacheProperties =>
          distinctHorizon

        case SingleRowLeaf(_) =>
          if (distinctHorizon.isRemovableDistinct) {
            aggregatingPlansToRemove.put(
              distinctHorizon.aggregatingPlan,
              distinctHorizon.aggregatingPlan.groupingExpressions.map { case (key, value) => key.name -> value }
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

    ReplacementPlans(aggregatingPlansToRemove.toMap, aggregatingPlansToRelax.toMap)
  }

  object SingleRowLeaf {

    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case _: Argument |
        _: DirectedRelationshipUniqueIndexSeek |
        _: NodeUniqueIndexSeek =>
        Some(plan)
      case _ =>
        None
    }
  }

  override def apply(input: AnyRef): AnyRef = {
    input match {
      case plan: LogicalPlan =>
        val replacementPlans = findReplacementPlans(plan)

        val innerRewriter = topDown(Rewriter.lift {

          case aggregation: Aggregation if replacementPlans.aggregatingPlansToRemove.contains(aggregation) =>
            Projection(
              aggregation.source,
              projectExpressions = replacementPlans.aggregatingPlansToRemove(aggregation).map {
                case (key, value) => varFor(key) -> value
              }
            )(SameId(aggregation.id))

          case aggregation: Aggregation if replacementPlans.aggregatingPlansToRelax.contains(aggregation) =>
            aggregation.copy(
              aggregationExpressions = replacementPlans.aggregatingPlansToRelax(aggregation).map {
                case (key, value) => varFor(key) -> value
              }
            )(
              SameId(aggregation.id)
            )

          case aggregation: OrderedAggregation if replacementPlans.aggregatingPlansToRemove.contains(aggregation) =>
            Projection(
              aggregation.source,
              projectExpressions = replacementPlans.aggregatingPlansToRemove(aggregation).map {
                case (key, value) => varFor(key) -> value
              }
            )(SameId(aggregation.id))

          case aggregation: OrderedAggregation if replacementPlans.aggregatingPlansToRelax.contains(aggregation) =>
            aggregation.copy(
              aggregationExpressions = replacementPlans.aggregatingPlansToRelax(aggregation).map {
                case (key, value) => varFor(key) -> value
              }
            )(
              SameId(aggregation.id)
            )

          case distinct: Distinct if replacementPlans.aggregatingPlansToRemove.contains(distinct) =>
            Projection(
              distinct.source,
              projectExpressions = distinct.groupingExpressions
            )(SameId(distinct.id))

          case distinct: OrderedDistinct if replacementPlans.aggregatingPlansToRemove.contains(distinct) =>
            Projection(
              distinct.source,
              projectExpressions = distinct.groupingExpressions
            )(SameId(distinct.id))
        })
        plan.endoRewrite(innerRewriter)

      case _ => input
    }
  }
}
