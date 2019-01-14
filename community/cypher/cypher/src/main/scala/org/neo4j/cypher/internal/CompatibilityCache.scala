/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.compatibility.v2_3.helpers._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compatibility.v3_3.{CommunityRuntimeContextCreator => CommunityRuntimeContextCreatorV3_3}
import org.neo4j.cypher.internal.compatibility.v3_4.Compatibility
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CommunityRuntimeBuilder, CommunityRuntimeContextCreator => CommunityRuntimeContextCreatorV3_4}
import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, v3_3 => v3_3compat}
import org.neo4j.cypher.internal.compiler.v3_4.CypherCompilerConfiguration
import org.neo4j.cypher.internal.util.v3_4.InvalidArgumentException
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

sealed trait PlannerSpec
final case class PlannerSpec_v2_3(planner: CypherPlanner, runtime: CypherRuntime) extends PlannerSpec
final case class PlannerSpec_v3_1(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_3(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_4(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec

trait CompatibilityFactory {
  def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility

  def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility

  def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): v3_3compat.Compatibility[_,_,_]

  def create(spec: PlannerSpec_v3_4, config: CypherCompilerConfiguration): Compatibility[_,_]
}

class CommunityCompatibilityFactory(graph: GraphDatabaseQueryService, kernelMonitors: KernelMonitors,
                                    logProvider: LogProvider) extends CompatibilityFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility = spec.planner match {
    case CypherPlanner.rule =>
      v2_3.RuleCompatibility(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors)
    case _ =>
      v2_3.CostCompatibility(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors, log, spec.planner, spec.runtime)
  }

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility = spec.planner match {
    case CypherPlanner.rule =>
      v3_1.RuleCompatibility(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors)
    case _ =>
      v3_1.CostCompatibility(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors, log, spec.planner, spec.runtime, spec.updateStrategy)
  }

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration) =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.3. If you need to use it, please select compatibility mode Cypher 3.1")
      case _ =>
        v3_3compat.Compatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, log,
          spec.planner, spec.runtime, spec.updateStrategy, CommunityRuntimeBuilder,
          CommunityRuntimeContextCreatorV3_3, CommunityRuntimeContextCreatorV3_4)
    }

  override def create(spec: PlannerSpec_v3_4, config: CypherCompilerConfiguration): Compatibility[_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.4. If you need to use it, please select compatibility mode Cypher 3.1")
      case _ =>
        Compatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, log,
                          spec.planner, spec.runtime, spec.updateStrategy, CommunityRuntimeBuilder,
                          CommunityRuntimeContextCreatorV3_4)
    }
}

class CompatibilityCache(factory: CompatibilityFactory) extends CompatibilityFactory {

  import scala.collection.convert.decorateAsScala._

  private val cache_v2_3 = new ConcurrentHashMap[PlannerSpec_v2_3, v2_3.Compatibility].asScala
  private val cache_v3_1 = new ConcurrentHashMap[PlannerSpec_v3_1, v3_1.Compatibility].asScala
  private val cache_v3_3 = new ConcurrentHashMap[PlannerSpec_v3_3, v3_3compat.Compatibility[_,_,_]].asScala
  private val cache_v3_4 = new ConcurrentHashMap[PlannerSpec_v3_4, Compatibility[_,_]].asScala

  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility =
    cache_v2_3.getOrElseUpdate(spec, factory.create(spec, config))

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility =
    cache_v3_1.getOrElseUpdate(spec, factory.create(spec, config))

  override def create(spec: PlannerSpec_v3_3, config: CypherCompilerConfiguration): v3_3compat.Compatibility[_,_,_] =
    cache_v3_3.getOrElseUpdate(spec, factory.create(spec, config))

  override def create(spec: PlannerSpec_v3_4, config: CypherCompilerConfiguration): Compatibility[_,_] =
    cache_v3_4.getOrElseUpdate(spec, factory.create(spec, config))
}
