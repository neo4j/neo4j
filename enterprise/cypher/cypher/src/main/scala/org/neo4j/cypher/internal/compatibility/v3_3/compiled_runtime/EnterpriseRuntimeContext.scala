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
package org.neo4j.cypher.internal.compatibility.v3_3.compiled_runtime

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_3.compiled_runtime.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.CommunityRuntimeContext
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{ExpressionEvaluator, Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_3.{ContextCreator, CypherCompilerConfiguration, SyntaxExceptionCreator, UpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InputPosition}
import org.neo4j.cypher.internal.v3_3.executionplan.GeneratedQuery

class EnterpriseRuntimeContext(override val exceptionCreator: (String, InputPosition) => CypherException,
                               override val tracer: CompilationPhaseTracer,
                               override val notificationLogger: InternalNotificationLogger,
                               override val planContext: PlanContext,
                               override val monitors: Monitors,
                               override val metrics: Metrics,
                               override val config: CypherCompilerConfiguration,
                               override val queryGraphSolver: QueryGraphSolver,
                               override val updateStrategy: UpdateStrategy,
                               override val debugOptions: Set[String],
                               override val clock: Clock,
                               val codeStructure: CodeStructure[GeneratedQuery])
  extends CommunityRuntimeContext(exceptionCreator, tracer,
                                  notificationLogger, planContext, monitors, metrics,
                                  config, queryGraphSolver, updateStrategy, debugOptions, clock)

case class EnterpriseRuntimeContextCreator(codeStructure: CodeStructure[GeneratedQuery]) extends ContextCreator[EnterpriseRuntimeContext] {

  override def create(tracer: CompilationPhaseTracer,
                      notificationLogger: InternalNotificationLogger,
                      planContext: PlanContext,
                      queryText: String,
                      debugOptions: Set[String],
                      offset: Option[InputPosition],
                      monitors: Monitors,
                      metricsFactory: MetricsFactory,
                      queryGraphSolver: QueryGraphSolver,
                      config: CypherCompilerConfiguration,
                      updateStrategy: UpdateStrategy,
                      clock: Clock,
                      evaluator: ExpressionEvaluator): EnterpriseRuntimeContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics, evaluator)

    new EnterpriseRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext,
                                monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, codeStructure)
  }
}