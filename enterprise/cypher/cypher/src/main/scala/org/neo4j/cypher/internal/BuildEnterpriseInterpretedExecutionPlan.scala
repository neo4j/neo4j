/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.RegisteredPipeBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions.EnterpriseExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_3.CypherCompilerConfiguration
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Monitors, Phase}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

object BuildEnterpriseInterpretedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    if (context.debugOptions.contains("sleipnir"))
      createRegisteredRuntimeExecPlan(from, context)
    else
      BuildInterpretedExecutionPlan.process(from, context)
  }

  private def createRegisteredRuntimeExecPlan(from: LogicalPlanState, context: EnterpriseRuntimeContext) = {
    val logicalPlan = from.logicalPlan
    val pipelines = RegisterAllocation.allocateRegisters(logicalPlan)
    val rewrittenPlan = logicalPlan.endoRewrite(new RegisteredRewriter(pipelines))
    val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
    val converters = new ExpressionConverters(CommunityExpressionConverter, EnterpriseExpressionConverters)
    ???
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors,
      expressionConverters = converters, pipeBuilderFactory = RegisteredPipeBuilderFactory())
    val pipeBuildContext = PipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(), from.plannerName)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan, idMap)(pipeBuildContext, context.planContext)
    val PipeInfo(pipe: Pipe, updating, periodicCommitInfo, fp, planner) = pipeInfo
    val columns = from.statement().returnColumns
    val resultBuilderFactory = DefaultExecutionResultBuilderFactory(pipeInfo, columns, logicalPlan, idMap)
    val func = BuildInterpretedExecutionPlan.getExecutionPlanFunction(periodicCommitInfo, from.queryText, updating, resultBuilderFactory, context.notificationLogger)
    val fingerprint = context.createFingerprintReference(fp)
    val periodicCommig = periodicCommitInfo.isDefined
    val indexes = logicalPlan.indexUsage
    val execPlan = RegisteredExecutionPlan(fingerprint, periodicCommig, planner, indexes, func, pipe, context.config)
    new CompilationState(from, Some(execPlan))
  }

  case class RegisteredExecutionPlan(fingerprint: PlanFingerprintReference,
                                     isPeriodicCommit: Boolean,
                                     plannerUsed: PlannerName,
                                     override val plannedIndexUsage: Seq[IndexUsage],
                                     runFunction: (QueryContext, ExecutionMode, Map[String, Any]) => InternalExecutionResult,
                                     pipe: Pipe,
                                     config: CypherCompilerConfiguration) extends executionplan.ExecutionPlan {
    override def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult =
      runFunction(queryContext, planType, params)

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = fingerprint.isStale(lastTxId, statistics)

    override def runtimeUsed = EnterpriseInterpretedRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] =
      BuildInterpretedExecutionPlan.checkForNotifications(pipe, planContext, config)
  }

  case class RegisteredPipeBuilderFactory() extends PipeBuilderFactory {
    def apply(monitors: Monitors, recurse: LogicalPlan => Pipe, readOnly: Boolean, idMap: Map[LogicalPlan, Id], expressionConverters: ExpressionConverters)
             (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder = {
      val fallback = CommunityPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)
      new RegisteredPipeBuilder(fallback)
    }
  }


}
