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
package org.neo4j.cypher.internal.compiler.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, MetricsFactory, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, CompilationPhaseTracer, InternalNotificationLogger, Monitors}

trait ContextCreator[Context <: BaseContext] {
  def create(tracer: CompilationPhaseTracer,
             notificationLogger: InternalNotificationLogger,
             planContext: PlanContext,
             queryText: String,
             debugOptions: Set[String],
             offset: Option[InputPosition],
             monitors: Monitors,
             createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
             typeConverter: RuntimeTypeConverter,
             metricsFactory: MetricsFactory,
             queryGraphSolver: QueryGraphSolver,
             config: CypherCompilerConfiguration,
             updateStrategy: UpdateStrategy,
             clock: Clock): Context
}

object CommunityContextCreator extends ContextCreator[CompilerContext] {
  override def create(tracer: CompilationPhaseTracer,
                      notificationLogger: InternalNotificationLogger,
                      planContext: PlanContext,
                      queryText: String,
                      debugOptions: Set[String],
                      offset: Option[InputPosition],
                      monitors: Monitors,
                      createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                      typeConverter: RuntimeTypeConverter,
                      metricsFactory: MetricsFactory,
                      queryGraphSolver: QueryGraphSolver,
                      config: CypherCompilerConfiguration,
                      updateStrategy: UpdateStrategy,
                      clock: Clock): CompilerContext = {
    val exceptionCreator = new SyntaxExceptionCreator(queryText, offset)

    val metrics: Metrics = if (planContext == null)
      null
    else
      metricsFactory.newMetrics(planContext.statistics)

    new CompilerContext(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference,
      monitors, metrics, queryGraphSolver, config, updateStrategy, debugOptions, clock)
  }
}
