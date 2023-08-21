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
package org.neo4j.cypher.internal.plandescription.rewrite

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.ArgumentPlanDescription
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.PipelineInfo
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.CompactedPlanDescription
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.NoChildren
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.plandescription.SingleChild
import org.neo4j.cypher.internal.plandescription.TwoChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait InternalPlanDescriptionRewriter {
  def rewrite(plan: InternalPlanDescription): InternalPlanDescription
}

/**
 * Aggregates arguments of fused pipelines, see rewrite method for more details.
 */
class FusedPlanDescriptionArgumentRewriter extends InternalPlanDescriptionRewriter {

  private case class AggregatedArguments(aggregation: Seq[Argument], aggregatedPlanIds: Set[Id])

  private case class PlanRewrite(replaceArguments: Seq[Argument], removeArgumentByName: Set[String])

  private class ArgumentRewriter(rewrites: Map[Id, PlanRewrite]) extends Rewriter {

    final private val instance: Rewriter = bottomUp(Rewriter.lift {
      case plan: InternalPlanDescription if rewrites.contains(plan.id) =>
        val rewrite = rewrites(plan.id)
        plan match {
          case planImpl: PlanDescriptionImpl => planImpl.copy(arguments = update(rewrite, planImpl.arguments))
          case argumentPlan: ArgumentPlanDescription =>
            argumentPlan.copy(arguments = update(rewrite, argumentPlan.arguments))
          case compactedPlan: CompactedPlanDescription => compactedPlan // Handled by the rewriter
          case _ => throw new IllegalStateException(s"Unexpected InternalPlanDescription type: $plan")
        }
    })

    private def update(rewrite: PlanRewrite, arguments: Seq[Argument]) = {
      val cleanedArguments = arguments.filterNot { argument =>
        rewrite.removeArgumentByName.contains(argument.name) ||
        rewrite.replaceArguments.exists(_.name == argument.name)
      }
      cleanedArguments ++ rewrite.replaceArguments
    }

    override def apply(that: AnyRef): AnyRef = instance.apply(that)
  }

  /**
   * Aggregates time and cache page hits/miss arguments of fused pipelines and assign the aggregate to
   * the first operator in each fused pipeline.
   *
   * For example, let's look at time if we have this plan before rewrite:
   *
   * +-------------------+-----------+---------------------+
   * | Operator          | Time (ms) | Other               |
   * +-------------------+-----------+---------------------+
   * | +ProduceResults   |     2.578 | In Pipeline 2       |
   * | |                 +-----------+---------------------+
   * | +Projection       |    12.005 | In Pipeline 1       |
   * | |                 +-----------+---------------------+
   * | +EagerAggregation |    10.451 | Fused in Pipeline 0 | <- EagerAggregation is blamed for some time by upstream operators
   * | |                 |           +---------------------+
   * | +Projection       |           | Fused in Pipeline 0 |
   * | |                 |           +---------------------+
   * | +NodeIndexScan    |    56.021 | Fused in Pipeline 0 | <- NodeIndexScan contains the recorded time for the complete fused pipeline 0
   * +-------------------+-----------+---------------------+
   *
   * we will get this after rewrite:
   *
   * +-------------------+-----------+---------------------+
   * | Operator          | Time (ms) | Other               |
   * +-------------------+-----------+---------------------+
   * | +ProduceResults   |     2.578 | In Pipeline 2       |
   * | |                 +-----------+---------------------+
   * | +Projection       |    12.005 | In Pipeline 1       |
   * | |                 +-----------+---------------------+
   * | +EagerAggregation |           | Fused in Pipeline 0 | <- This time has been added to first operator in fused pipeline 0
   * | |                 |           +---------------------+
   * | +Projection       |           | Fused in Pipeline 0 |
   * | |                 |           +---------------------+
   * | +NodeIndexScan    |    66.472 | Fused in Pipeline 0 | <- NodeIndexScan time now contains the sum of time in fused pipeline 0
   * +-------------------+-----------+---------------------+
   *
   */
  override def rewrite(root: InternalPlanDescription): InternalPlanDescription = {
    var currentPipelineInfo: Option[PipelineInfo] = None
    var plansToAggregate = mutable.ArrayBuffer.empty[InternalPlanDescription]
    val stack = new mutable.Stack[InternalPlanDescription]
    val rewritesByPlanId = mutable.Map.empty[Id, PlanRewrite]

    def computePipelineArgumentAggregates(): Unit = {
      val skipLastCount = plansToAggregate.reverseIterator.takeWhile(plan => pipelineInfo(plan).isEmpty).size
      // Remove trailing plans without info, we only aggregate plans without info if they fall between operators of a fused pipeline
      plansToAggregate = plansToAggregate.dropRight(skipLastCount)
      if (plansToAggregate.size > 1) {
        val headPlanId = plansToAggregate.last.id
        val aggregatedArguments = aggregateArguments(plansToAggregate)

        // Rewrite for inserting aggregated arguments at head plan
        val replaceRewrite = headPlanId -> PlanRewrite(aggregatedArguments.aggregation, Set.empty)

        // Rewrite for removing arguments that will be aggregated in head plan
        val argumentNamesToRemove = aggregatedArguments.aggregation.map(_.name).toSet
        val removeRewrites = aggregatedArguments.aggregatedPlanIds
          .filter(_ != headPlanId)
          .map(planId => planId -> PlanRewrite(Seq.empty, argumentNamesToRemove))

        rewritesByPlanId += replaceRewrite
        rewritesByPlanId ++= removeRewrites

        plansToAggregate.clear()
      }
    }

    stack.push(root)
    while (stack.nonEmpty) {
      val plan = stack.pop()
      plan.children match {
        case NoChildren =>
        // do nothing
        case SingleChild(child) =>
          stack.push(child)
        case TwoChildren(lhs, rhs) =>
          stack.push(lhs)
          stack.push(rhs)
      }

      pipelineInfo(plan) match {
        case Some(newInfo) =>
          if (!currentPipelineInfo.contains(newInfo)) {
            // We're not in the same fused pipeline anymore, calculate aggregation for previous fused pipeline (if any)
            computePipelineArgumentAggregates()
          }
          if (newInfo.fused) {
            plansToAggregate += plan
          }
          currentPipelineInfo = Some(newInfo)
        case None =>
          if (plansToAggregate.nonEmpty) {
            // Plans without info are aggregated if they fall between other plans of the same fused pipeline.
            plansToAggregate += plan
          }
      }
    }
    computePipelineArgumentAggregates()

    root.endoRewrite(new ArgumentRewriter(rewritesByPlanId.toMap))
  }

  private def aggregateArguments(plans: ArrayBuffer[InternalPlanDescription]): AggregatedArguments = {
    var time = 0L
    var hits = 0L
    var misses = 0L
    val planIds = mutable.Set.empty[Id]
    plans.foreach { plan =>
      plan.arguments.foreach {
        case Time(value) =>
          time += value
          planIds += plan.id
        case PageCacheHits(value) =>
          hits += value
          planIds += plan.id
        case PageCacheMisses(value) =>
          misses += value
          planIds += plan.id
        case _ => // Do nothing
      }
    }
    AggregatedArguments(Seq(Time(time), PageCacheHits(hits), PageCacheMisses(misses)), planIds.toSet)
  }

  private def pipelineInfo(plan: InternalPlanDescription): Option[PipelineInfo] = {
    plan.arguments.collectFirst { case info: PipelineInfo => info }
  }
}
