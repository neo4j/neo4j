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

import org.neo4j.cypher.internal.compatibility.v3_3.compiled_runtime.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.RuntimeSpecificContext
import org.neo4j.cypher.internal.compatibility.v3_3.{Compatibility, CostCompatibility}
import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, v3_2}
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.phases.CompilerContext
import org.neo4j.cypher.internal.spi.v3_3.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherPlanner, CypherRuntime}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.LogProvider

class EnterpriseCompatibilityFactory(inner: CompatibilityFactory, graph: GraphDatabaseQueryService,
                                     kernelAPI: KernelAPI, kernelMonitors: KernelMonitors,
                                     logProvider: LogProvider) extends CompatibilityFactory {
  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_2, config: CypherCompilerConfiguration): v3_2.Compatibility[_] =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): Compatibility[_,_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) => inner.create(spec, config)

      case (_, CypherRuntime.compiled) | (_, CypherRuntime.default) =>
        val runtimeContextCreator = (c: CompilerContext, data: RuntimeSpecificContext) =>  {
          new EnterpriseRuntimeContext(c.exceptionCreator, c.tracer, c.notificationLogger, c.planContext, c.monitors,
                                       c.metrics, c.queryGraphSolver, c.config, c.updateStrategy, c.debugOptions, c.clock,
                                       data.createFingerprintReference, GeneratedQueryStructure)
        }

        CostCompatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI, logProvider.getLog(getClass),
                          spec.planner, spec.runtime, spec.updateStrategy, EnterpriseRuntimeBuilder, LogicalPlanningContextCreator,
                          runtimeContextCreator)

      case _ => inner.create(spec, config)
    }
}
