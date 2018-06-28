/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlanv3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.InternalWrapping.asKernelNotification
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CompiledRuntimeName, RuntimeName}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenerator}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.Notification
import org.neo4j.values.virtual.MapValue

object CompiledRuntime extends CypherRuntime[EnterpriseRuntimeContext] {

  @throws[CantCompileQueryException]
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlanv3_5 = {
    val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
    val compiled: CompiledPlan = codeGen.generate(
      state.logicalPlan,
      context.tokenContext,
      state.semanticTable(),
      state.plannerName,
      context.readOnly,
      state.cardinalities)
    new CompiledExecutionPlan(
      compiled,
      notifications(context))
  }

  private def notifications(context: EnterpriseRuntimeContext): Set[Notification] = {
    val mapper = asKernelNotification(context.notificationLogger.offset) _
    context.notificationLogger.notifications.map(mapper)
  }

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan,
                              val notifications: Set[Notification]) extends ExecutionPlanv3_5 {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue): RuntimeResult = {

      val executionMode = if (doProfile) ProfileMode else NormalMode
      val tracer =
        if (doProfile) Some(new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider))
        else None

      compiled.executionResultBuilder(queryContext, executionMode, tracer, params)
    }

    override val runtimeName: RuntimeName = CompiledRuntimeName
  }
}
