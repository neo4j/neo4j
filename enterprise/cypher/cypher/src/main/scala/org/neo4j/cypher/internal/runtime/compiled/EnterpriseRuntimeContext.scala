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
package org.neo4j.cypher.internal.runtime.compiled

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.CommunityRuntimeContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{ExpressionEvaluator, Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_5.{ContextCreator, CypherPlannerConfiguration, SyntaxExceptionCreator, UpdateStrategy}
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.opencypher.v9_0.frontend.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.Dispatcher
import org.neo4j.logging.Log
import org.opencypher.v9_0.util.attribution.IdGen
import org.opencypher.v9_0.util.{CypherException, InputPosition}

class EnterpriseRuntimeContext(override val exceptionCreator: (String, InputPosition) => CypherException,
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
                               override val logicalPlanIdGen: IdGen,
                               val codeStructure: CodeStructure[GeneratedQuery],
                               val dispatcher: Dispatcher,
                               val log: Log)
  extends CommunityRuntimeContext(exceptionCreator, tracer,
                                  notificationLogger, planContext, monitors, metrics,
                                  config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen)

case class EnterpriseRuntimeContextCreator(codeStructure: CodeStructure[GeneratedQuery], dispatcher: Dispatcher, log: Log) extends ContextCreator[EnterpriseRuntimeContext] {

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
                      evaluator: ExpressionEvaluator): EnterpriseRuntimeContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics, evaluator, config)

    new EnterpriseRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext,
                                monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen, codeStructure, dispatcher, log)
  }
}
