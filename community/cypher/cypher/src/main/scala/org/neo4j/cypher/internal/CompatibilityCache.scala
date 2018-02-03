/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CommunityRuntimeBuilder, CommunityRuntimeContextCreator}
import org.neo4j.cypher.internal.compatibility.v3_3.{Compatibility, CostCompatibility}
import org.neo4j.cypher.internal.compiler.v3_3.CypherCompilerConfiguration
import org.neo4j.cypher.internal.frontend.v3_3.InvalidArgumentException
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

import scala.collection.mutable

sealed trait PlannerSpec
final case class PlannerSpec_v3_3(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec

trait CompatibilityFactory {
  def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): Compatibility[_,_]
}

class CommunityCompatibilityFactory(graph: GraphDatabaseQueryService, kernelAPI: KernelAPI, kernelMonitors: KernelMonitors,
                                    logProvider: LogProvider) extends CompatibilityFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): Compatibility[_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.3. If you need to use it, please compatibility mode Cypher 3.1")
      case _ =>
        CostCompatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI, log,
                          spec.planner, spec.runtime, spec.updateStrategy, CommunityRuntimeBuilder,
                          CommunityRuntimeContextCreator)
    }
}

class CompatibilityCache(factory: CompatibilityFactory) extends CompatibilityFactory {
  private val cache_v3_3 = new mutable.HashMap[PlannerSpec_v3_3, Compatibility[_,_]]

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): Compatibility[_,_] =
    cache_v3_3.getOrElseUpdate(spec, factory.create(spec, config))
}
