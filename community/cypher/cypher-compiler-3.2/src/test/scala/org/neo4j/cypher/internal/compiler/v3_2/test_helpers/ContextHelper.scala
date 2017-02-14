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
package org.neo4j.cypher.internal.compiler.v3_2.test_helpers

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors, devNullLogger}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, InternalException}
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {
  def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => new InternalException("apa"),
             tracer: CompilationPhaseTracer = NO_TRACING,
             notificationLogger: InternalNotificationLogger = devNullLogger,
             planContext: PlanContext = new NotImplementedPlanContext,
             typeConverter: RuntimeTypeConverter = mock[RuntimeTypeConverter],
             createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference = _ => mock[PlanFingerprintReference],
             monitors: Monitors = mock[Monitors],
             metrics: Metrics = mock[Metrics],
             queryGraphSolver: QueryGraphSolver = mock[QueryGraphSolver],
             config: CypherCompilerConfiguration = mock[CypherCompilerConfiguration],
             updateStrategy: UpdateStrategy = mock[UpdateStrategy],
             clock: Clock = Clock.systemUTC()): CompilerContext = {
    new CompilerContext(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference,
      monitors, metrics, queryGraphSolver, config, updateStrategy, clock)
  }
}
