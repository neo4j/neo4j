/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import java.time.Clock

import org.opencypher.v9_0.util.{CypherException, InputPosition}
import org.neo4j.cypher.internal.compiler.v3_5.phases.PlannerContext
import org.opencypher.v9_0.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{ExpressionEvaluator, Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_5.{ContextCreator, CypherPlannerConfiguration, SyntaxExceptionCreator, UpdateStrategy}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.opencypher.v9_0.util.attribution.IdGen

class CommunityRuntimeContext(override val exceptionCreator: (String, InputPosition) => CypherException,
                              override val tracer: CompilationPhaseTracer,
                              override val notificationLogger: InternalNotificationLogger,
                              override val planContext: PlanContext,
                              override val monitors: Monitors,
                              override val metrics: Metrics,
                              override val config: CypherPlannerConfiguration,
                              override val queryGraphSolver: QueryGraphSolver,
                              override val updateStrategy: UpdateStrategy,
                              override val debugOptions: Set[String],
                              override val clock: Clock,
                              override val logicalPlanIdGen: IdGen)
  extends PlannerContext(exceptionCreator, tracer,
                          notificationLogger, planContext, monitors, metrics,
                          config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen)

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
                      config: CypherPlannerConfiguration,
                      updateStrategy: UpdateStrategy,
                      clock: Clock,
                      logicalPlanIdGen: IdGen,
                      evaluator: ExpressionEvaluator): CommunityRuntimeContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics, evaluator, config)

    new CommunityRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext,
                        monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen)
  }
}

