/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.PlanFingerprint
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.PlanFingerprintReference
import org.neo4j.cypher.internal.compiler.v3_3.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_3.ContextCreator
import org.neo4j.cypher.internal.compiler.v3_3.CypherCompilerConfiguration
import org.neo4j.cypher.internal.compiler.v3_3.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.v3_3.UpdateStrategy
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.CypherException
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition

class CommunityRuntimeContext(override val exceptionCreator: (String, InputPosition) => CypherException,
                              override val tracer: CompilationPhaseTracer,
                              override val notificationLogger: InternalNotificationLogger,
                              override val planContext: PlanContext,
                              override val monitors: Monitors,
                              override val metrics: Metrics,
                              override val config: CypherCompilerConfiguration,
                              override val queryGraphSolver: QueryGraphSolver,
                              override val updateStrategy: UpdateStrategy,
                              override val debugOptions: Set[String],
                              override val clock: Clock)
    extends CompilerContext(exceptionCreator,
                            tracer,
                            notificationLogger,
                            planContext,
                            monitors,
                            metrics,
                            config,
                            queryGraphSolver,
                            updateStrategy,
                            debugOptions,
                            clock) {

  val createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference =
    new PlanFingerprintReference(clock, config.queryPlanTTL, config.statsDivergenceThreshold, _)
}

object CommunityRuntimeContextCreator extends ContextCreator[CommunityRuntimeContext] {

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
                      evaluator: ExpressionEvaluator): CommunityRuntimeContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics =
      if (planContext == null)
        null
      else
        metricsFactory.newMetrics(planContext.statistics, evaluator)

    new CommunityRuntimeContext(exceptionCreator,
                                tracer,
                                notificationLogger,
                                planContext,
                                monitors,
                                metrics,
                                config,
                                queryGraphSolver,
                                updateStrategy,
                                debugOptions,
                                clock)
  }
}
