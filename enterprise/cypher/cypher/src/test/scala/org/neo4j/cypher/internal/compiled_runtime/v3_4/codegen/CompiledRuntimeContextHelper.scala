/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4.codegen

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_4.{CypherCompilerConfiguration, NotImplementedPlanContext, UpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors, devNullLogger}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.SingleThreadedExecutor
import org.neo4j.cypher.internal.util.v3_4.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.util.v3_4.{CypherException, InputPosition, InternalException}
import org.neo4j.cypher.internal.v3_4.executionplan.GeneratedQuery
import org.scalatest.mock.MockitoSugar

object CompiledRuntimeContextHelper extends MockitoSugar {
    def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => new InternalException("apa"),
               tracer: CompilationPhaseTracer = NO_TRACING,
               notificationLogger: InternalNotificationLogger = devNullLogger,
               planContext: PlanContext = new NotImplementedPlanContext,
               createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference = _ => mock[PlanFingerprintReference],
               monitors: Monitors = mock[Monitors],
               metrics: Metrics = mock[Metrics],
               queryGraphSolver: QueryGraphSolver = mock[QueryGraphSolver],
               config: CypherCompilerConfiguration = mock[CypherCompilerConfiguration],
               updateStrategy: UpdateStrategy = mock[UpdateStrategy],
               debugOptions: Set[String] = Set.empty,
               clock: Clock = Clock.systemUTC(),
               logicalPlanIdGen: IdGen = new SequentialIdGen(),
               codeStructure: CodeStructure[GeneratedQuery] = mock[CodeStructure[GeneratedQuery]]): EnterpriseRuntimeContext = {
      new EnterpriseRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext,
                                   monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen, codeStructure,
                                   new SingleThreadedExecutor())
    }

}
