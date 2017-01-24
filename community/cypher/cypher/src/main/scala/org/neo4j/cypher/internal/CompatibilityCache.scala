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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v2_3.helpers._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, _}
import org.neo4j.cypher.internal.compiler.v3_2.CypherCompilerConfiguration
import org.neo4j.cypher.internal.frontend.v3_2.InvalidArgumentException
import org.neo4j.cypher.{CypherCodeGenMode, CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

import scala.collection.mutable

sealed trait PlannerSpec
final case class PlannerSpec_v2_3(planner: CypherPlanner, runtime: CypherRuntime) extends PlannerSpec
final case class PlannerSpec_v3_1(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_2(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy, codeGenMode: CypherCodeGenMode) extends PlannerSpec

trait CompatibilityFactory {
  def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility

  def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility

  def create(spec: PlannerSpec_v3_2, config: CypherCompilerConfiguration): v3_2.Compatibility
}

class CommunityCompatibilityFactory(graph: GraphDatabaseQueryService, kernelAPI: KernelAPI, kernelMonitors: KernelMonitors,
                                    logProvider: LogProvider) extends CompatibilityFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility = spec.planner match {
    case CypherPlanner.rule =>
      v2_3.RuleCompatibility(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors, kernelAPI)
    case _ =>
      v2_3.CostCompatibility(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime)
  }

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility = spec.planner match {
    case CypherPlanner.rule =>
      v3_1.RuleCompatibility(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI)
    case _ =>
      v3_1.CostCompatibility(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime, spec.updateStrategy)
  }

  override def create(spec: PlannerSpec_v3_2, config: CypherCompilerConfiguration): v3_2.Compatibility =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.2. If you need to use it, please compatibility mode Cypher 3.1")
      case (_, CypherRuntime.compiled) =>
        throw new InvalidArgumentException("The compiled runtime is only available in the Enterprise version of Neo4j")
      case _ =>
        v3_2.CostCompatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime, spec.codeGenMode, spec.updateStrategy)
    }
}

class CompatibilityCache(factory: CompatibilityFactory) extends CompatibilityFactory {
  private val cache_v2_3 = new mutable.HashMap[PlannerSpec_v2_3, v2_3.Compatibility]
  private val cache_v3_1 = new mutable.HashMap[PlannerSpec_v3_1, v3_1.Compatibility]
  private val cache_v3_2 = new mutable.HashMap[PlannerSpec_v3_2, v3_2.Compatibility]

  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility =
    cache_v2_3.getOrElseUpdate(spec, factory.create(spec, config))

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility =
    cache_v3_1.getOrElseUpdate(spec, factory.create(spec, config))

  override def create(spec: PlannerSpec_v3_2, config: CypherCompilerConfiguration): v3_2.Compatibility =
    cache_v3_2.getOrElseUpdate(spec, factory.create(spec, config))
}
