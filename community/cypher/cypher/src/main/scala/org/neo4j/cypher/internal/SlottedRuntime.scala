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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.InterpretedRuntime.InterpretedExecutionPlan
import org.neo4j.cypher.internal.InterpretedRuntime.calculateTransactionMode
import org.neo4j.cypher.internal.SlottedRuntime.NO_METADATA
import org.neo4j.cypher.internal.SlottedRuntime.NO_WARNINGS
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanner
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.QueryIndexRegistrator
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExpressions
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTreeBuilder
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionResultBuilderFactory
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipelineBreakingPolicy
import org.neo4j.cypher.internal.runtime.slotted.expressions.MaterializedEntitiesExpressionConverter
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.exceptions.CantCompileQueryException

trait SlottedRuntime[-CONTEXT <: RuntimeContext] extends CypherRuntime[CONTEXT] with DebugPrettyPrinter {
  override def name: String = "slotted"

  override def correspondingRuntimeOption: Option[CypherRuntimeOption] = Some(CypherRuntimeOption.slotted)

  val ENABLE_DEBUG_PRINTS = false // NOTE: false toggles all debug prints off, overriding the individual settings below

  // Should we print query text and logical plan before we see any exceptions from execution plan building?
  // Setting this to true is useful if you want to see the query and logical plan while debugging a failure
  // Setting this to false is useful if you want to quickly spot the failure reason at the top of the output from tests
  val PRINT_PLAN_INFO_EARLY = true

  override val PRINT_QUERY_TEXT = true
  override val PRINT_LOGICAL_PLAN = true
  override val PRINT_REWRITTEN_LOGICAL_PLAN = true
  override val PRINT_PIPELINE_INFO = true
  override val PRINT_FAILURE_STACK_TRACE = true

  protected def compileExpressions(
    baseConverters: List[ExpressionConverter],
    context: CONTEXT,
    physicalPlan: PhysicalPlan,
    query: LogicalQuery,
    selectivityTrackerRegistrator: SelectivityTrackerRegistrator
  ): (Option[ExpressionConverter], List[ExpressionConverter], () => Seq[Argument], () => Set[InternalNotification]) = {
    (None, baseConverters, NO_METADATA, NO_WARNINGS)
  }

  @throws[CantCompileQueryException]
  override def compileToExecutable(query: LogicalQuery, context: CONTEXT): ExecutionPlan = {
    try {
      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printPlanInfo(query)
      }

      val physicalPlan = PhysicalPlanner.plan(
        context.tokenContext,
        query.logicalPlan,
        query.semanticTable,
        SlottedPipelineBreakingPolicy,
        context.config,
        context.anonymousVariableNameGenerator,
        () => context.assertOpen.assertOpen()
      )

      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printRewrittenPlanInfo(physicalPlan.logicalPlan)
      }

      val selectivityTrackerRegistrator = new SelectivityTrackerRegistrator()
      val baseConverters = List(
        SlottedExpressionConverters(physicalPlan),
        CommunityExpressionConverter(
          context.tokenContext,
          context.anonymousVariableNameGenerator,
          selectivityTrackerRegistrator,
          context.config
        )
      )

      val (mainConverter, fallbackConverters, metadataGen, warningsGen) =
        if (context.materializedEntitiesMode) {
          val converters = MaterializedEntitiesExpressionConverter(context.tokenContext) +: baseConverters
          (None, converters, NO_METADATA, NO_WARNINGS)
        } else if (context.compileExpressions) {
          compileExpressions(baseConverters, context, physicalPlan, query, selectivityTrackerRegistrator)
        } else {
          (None, baseConverters, NO_METADATA, NO_WARNINGS)
        }

      val converters = new ExpressionConverters(mainConverter, fallbackConverters: _*)

      val queryIndexRegistrator = new QueryIndexRegistrator(context.schemaRead)
      val fallback = InterpretedPipeMapper(
        context.cypherVersion,
        query.readOnly,
        converters,
        context.tokenContext,
        queryIndexRegistrator,
        context.anonymousVariableNameGenerator,
        context.isCommunity,
        physicalPlan.parameterMapping
      )(query.semanticTable)
      val pipeBuilder = new SlottedPipeMapper(
        fallback,
        converters,
        physicalPlan,
        query.readOnly,
        queryIndexRegistrator
      )(query.semanticTable)
      val pipeTreeBuilder = PipeTreeBuilder(pipeBuilder)
      val logicalPlanWithConvertedNestedPlans = NestedPipeExpressions.build(
        pipeTreeBuilder,
        physicalPlan.logicalPlan,
        physicalPlan.availableExpressionVariables,
        () => context.assertOpen.assertOpen()
      )
      val pipe = pipeTreeBuilder.build(logicalPlanWithConvertedNestedPlans, () => context.assertOpen.assertOpen())
      val columns = query.resultColumns

      val transactionMode = calculateTransactionMode(query)

      val resultBuilderFactory =
        new SlottedExecutionResultBuilderFactory(
          pipe,
          queryIndexRegistrator.result(),
          selectivityTrackerRegistrator.result(),
          physicalPlan.nExpressionSlots,
          columns,
          physicalPlan.parameterMapping,
          context.config.lenientCreateRelationship,
          context.config.memoryTrackingController,
          query.hasLoadCSV,
          transactionMode
        )

      if (ENABLE_DEBUG_PRINTS) {
        if (!PRINT_PLAN_INFO_EARLY) {
          // Print after execution plan building to see any occurring exceptions first
          printPlanInfo(query)
          printRewrittenPlanInfo(physicalPlan.logicalPlan)
        }
        printPipe(physicalPlan.slotConfigurations, pipe)
      }

      new InterpretedExecutionPlan(
        resultBuilderFactory,
        SlottedRuntimeName,
        query.readOnly,
        transactionMode.startsTransactions,
        metadataGen(),
        warningsGen()
      )
    } catch {
      case e: CypherException =>
        if (ENABLE_DEBUG_PRINTS) {
          printFailureStackTrace(e)
          if (!PRINT_PLAN_INFO_EARLY) {
            printPlanInfo(query)
          }
        }
        throw e
    }
  }
}

object SlottedRuntime {
  val NO_WARNINGS: () => Set[InternalNotification] = () => Set.empty[InternalNotification]
  val NO_METADATA: () => Seq[Argument] = () => Seq.empty[Argument]
}

object CommunitySlottedRuntime extends SlottedRuntime[RuntimeContext]
