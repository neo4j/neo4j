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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeImpl
import org.neo4j.cypher.internal.plandescription.Arguments.StringRepresentation
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.rewrite.InternalPlanDescriptionRewriter
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.kernel.api.query.RuntimeName

import scala.util.chaining.scalaUtilChainingOps

object PlanDescriptionBuilder {

  def apply(
    logicalPlan: LogicalPlan,
    plannerName: PlannerName,
    readOnly: Boolean,
    effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
    withRawCardinalities: Boolean,
    withDistinctness: Boolean,
    renderNestedPlanExpressions: Boolean,
    providedOrders: ImmutablePlanningAttributes.ProvidedOrders,
    executionPlan: ExecutionPlan,
    renderPlanDescription: Boolean,
    cypherVersion: CypherVersion,
    explainScopeOpt: Option[WorkingScope],
    cypherPlannerVersion: Option[String]
  ): PlanDescriptionBuilder = {
    // NOTE: We should not keep a reference to the ExecutionPlan in the PlanDescriptionBuilder since it can end up in long-lived caches, e.g. RecentQueryBuffer
    val batchSize = executionPlan.maybeBatchSize
    val runtimeName = executionPlan.runtimeName
    val runtimeMetadata = executionPlan.metadata
    val runtimeOperatorMetadata = executionPlan.operatorMetadata
    val internalPlanDescriptionRewriter = executionPlan.internalPlanDescriptionRewriter

    new PlanDescriptionBuilder(
      logicalPlan: LogicalPlan,
      plannerName: PlannerName,
      readOnly,
      effectiveCardinalities,
      withRawCardinalities,
      withDistinctness,
      renderNestedPlanExpressions,
      providedOrders,
      runtimeName,
      runtimeMetadata,
      runtimeOperatorMetadata,
      internalPlanDescriptionRewriter,
      batchSize,
      renderPlanDescription,
      cypherVersion,
      explainScopeOpt,
      cypherPlannerVersion
    )
  }
}

class PlanDescriptionBuilder(
  logicalPlan: LogicalPlan,
  plannerName: PlannerName,
  readOnly: Boolean,
  effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
  withRawCardinalities: Boolean,
  withDistinctness: Boolean,
  renderNestedPlanExpressions: Boolean,
  providedOrders: ImmutablePlanningAttributes.ProvidedOrders,
  runtimeName: RuntimeName,
  runtimeMetadata: Seq[Argument],
  runtimeOperatorMetadata: Id => Seq[Argument],
  internalPlanDescriptionRewriter: Option[InternalPlanDescriptionRewriter],
  batchSize: Option[Int],
  includeStringRepresentation: Boolean,
  cypherVersion: CypherVersion,
  explainScopeOpt: Option[WorkingScope],
  cypherPlannerVersion: Option[String]
) {

  def scope(): InternalPlanDescription = {
    if (explainScopeOpt.nonEmpty)
      WorkingScope2PlanDescription(explainScopeOpt.get)
    else
      explain()
  }

  def explain(): InternalPlanDescription = {
    val description =
      LogicalPlan2PlanDescription
        .create(
          logicalPlan,
          plannerName,
          readOnly,
          effectiveCardinalities,
          withRawCardinalities,
          withDistinctness,
          renderNestedPlanExpressions,
          providedOrders,
          runtimeOperatorMetadata,
          cypherVersion,
          cypherPlannerVersion
        )
        .addArgument(Runtime(runtimeName.toTextOutput))
        .addArgument(RuntimeImpl(runtimeName.toTextOutput))

    val withMetaData = (runtimeMetadata ++ batchSize.map(BatchSize.apply)).foldLeft(description)((plan, metadata) =>
      plan.addArgument(metadata)
    )

    if (includeStringRepresentation) {
      withMetaData.addArgument(StringRepresentation(withMetaData.toString))
    } else {
      withMetaData
    }
  }

  def profile(queryProfile: QueryProfile): InternalPlanDescription = {

    val planDescription = BuildPlanDescription(explain())
      .addArgument(Arguments.GlobalMemory.apply, queryProfile.maxAllocatedMemory())
      .addArgument(Arguments.AvailableWorkers.apply, queryProfile.numberOfAvailableWorkers())
      .addArgument(Arguments.AvailableProcessors.apply, queryProfile.numberOfAvailableProcessors())
      .plan
      .map { (input: InternalPlanDescription) =>
        val data = queryProfile.operatorProfile(input.id.x)

        BuildPlanDescription(input)
          .addArgument(Arguments.Rows.apply, data.rows)
          .addArgument(Arguments.DbHits.apply, data.dbHits)
          .addArgument(Arguments.PageCacheHits.apply, data.pageCacheHits)
          .addArgument(Arguments.PageCacheMisses.apply, data.pageCacheMisses)
          .addArgument(Time.apply, data.time())
          .addArgument(Arguments.Memory.apply, data.maxAllocatedMemory())
          .pipe { plan =>
            val indexes = data.indexesUsed().zip(data.indexUseCount())
            if (indexes.nonEmpty) {
              plan.addArgument(
                Arguments.UsedIndexes.apply,
                indexes.map { case (idx, count) => idx.getName -> count }.toMap
              )
            } else plan
          }
          .plan
      }

    val rewritten = internalPlanDescriptionRewriter match {
      case Some(rewriter) => rewriter.rewrite(planDescription)
      case None           => planDescription
    }
    if (includeStringRepresentation) {
      rewritten.addArgument(StringRepresentation(rewritten.toString))
    } else {
      rewritten
    }
  }

  case class BuildPlanDescription(plan: InternalPlanDescription) {

    def addArgument[T](argument: T => Argument, value: T): BuildPlanDescription =
      if (value == OperatorProfile.NO_DATA) {
        this
      } else {
        BuildPlanDescription(plan.addArgument(argument(value)))
      }
  }
}
