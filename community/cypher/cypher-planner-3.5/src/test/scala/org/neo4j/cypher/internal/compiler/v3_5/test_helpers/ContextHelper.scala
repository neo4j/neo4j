/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.test_helpers

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.v3_5.frontend.phases.{InternalNotificationLogger, Monitors, devNullLogger}
import org.neo4j.cypher.internal.v3_5.util.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.v3_5.util.{CypherException, InputPosition, InternalException}
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {
  def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => new InternalException("apa"),
             tracer: CompilationPhaseTracer = NO_TRACING,
             notificationLogger: InternalNotificationLogger = devNullLogger,
             planContext: PlanContext = new NotImplementedPlanContext,
             monitors: Monitors = mock[Monitors],
             metrics: Metrics = mock[Metrics],
             config: CypherPlannerConfiguration = mock[CypherPlannerConfiguration],
             queryGraphSolver: QueryGraphSolver = mock[QueryGraphSolver],
             updateStrategy: UpdateStrategy = mock[UpdateStrategy],
             debugOptions: Set[String] = Set.empty,
             clock: Clock = Clock.systemUTC(),
             logicalPlanIdGen: IdGen = new SequentialIdGen()): PlannerContext = {
    new PlannerContext(exceptionCreator, tracer, notificationLogger, planContext,
      monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen)
  }
}
