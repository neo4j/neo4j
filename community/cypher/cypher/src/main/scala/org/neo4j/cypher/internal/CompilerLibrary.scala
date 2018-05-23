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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v2_3.helpers._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compatibility.v3_3.{CommunityRuntimeContextCreator => CommunityRuntimeContextCreatorV3_3}
import org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Compiler
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CommunityRuntimeBuilder, CommunityRuntimeContextCreator => CommunityRuntimeContextCreatorv3_5}
import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, v3_3 => v3_3compat}
import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.opencypher.v9_0.util.InvalidArgumentException
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

sealed trait PlannerSpec
final case class PlannerSpec_v2_3(planner: CypherPlannerOption, runtime: CypherRuntimeOption) extends PlannerSpec
final case class PlannerSpec_v3_1(planner: CypherPlannerOption, runtime: CypherRuntimeOption, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_3(planner: CypherPlannerOption, runtime: CypherRuntimeOption, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_5(planner: CypherPlannerOption, runtime: CypherRuntimeOption, updateStrategy: CypherUpdateStrategy) extends PlannerSpec

trait CompilerFactory {
  def create(spec: PlannerSpec_v2_3, config: CypherPlannerConfiguration): v2_3.Cypher23Compiler

  def create(spec: PlannerSpec_v3_1, config: CypherPlannerConfiguration): v3_1.Cypher31Compiler

  def create(spec: PlannerSpec_v3_3, config: CypherPlannerConfiguration): v3_3compat.Cypher33Compiler[_,_,_]

  def create(spec: PlannerSpec_v3_5, config: CypherPlannerConfiguration): Cypher35Compiler[_,_]

  def createCompiler(cypherVersion: CypherVersion,
                     cypherPlanner: CypherPlannerOption,
                     cypherRuntime: CypherRuntimeOption,
                     cypherUpdateStrategy: CypherUpdateStrategy,
                     config: CypherPlannerConfiguration): Compiler = {
    cypherVersion match {
      case CypherVersion.v2_3 => create(PlannerSpec_v2_3(cypherPlanner, cypherRuntime), config)
      case CypherVersion.v3_1 => create(PlannerSpec_v3_1(cypherPlanner, cypherRuntime, cypherUpdateStrategy), config)
      case CypherVersion.v3_3 => create(PlannerSpec_v3_3(cypherPlanner, cypherRuntime, cypherUpdateStrategy), config)
      case CypherVersion.v3_5 => create(PlannerSpec_v3_5(cypherPlanner, cypherRuntime, cypherUpdateStrategy), config)
    }
  }
}

class CommunityCompatibilityFactory(graph: GraphDatabaseQueryService, kernelMonitors: KernelMonitors,
                                    logProvider: LogProvider) extends CompilerFactory {

  private val log: Log = logProvider.getLog(getClass)

  override def create(spec: PlannerSpec_v2_3, config: CypherPlannerConfiguration): v2_3.Cypher23Compiler = spec.planner match {
    case CypherPlannerOption.rule =>
      v2_3.Rule23Compiler(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors)
    case _ =>
      v2_3.Cost23Compiler(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors, log, spec.planner, spec.runtime)
  }

  override def create(spec: PlannerSpec_v3_1, config: CypherPlannerConfiguration): v3_1.Cypher31Compiler = spec.planner match {
    case CypherPlannerOption.rule =>
      v3_1.Rule31Compiler(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors)
    case _ =>
      v3_1.Cost31Compiler(graph, as3_1(config), CompilerEngineDelegator.CLOCK, kernelMonitors, log, spec.planner, spec.runtime, spec.updateStrategy)
  }

  override def create(spec: PlannerSpec_v3_3, config: CypherPlannerConfiguration): v3_3compat.Cypher33Compiler[_,_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlannerOption.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.3. If you need to use it, please select compatibility mode Cypher 3.1")
      case _ =>
        v3_3compat.Cypher33Compiler(config, CompilerEngineDelegator.CLOCK, kernelMonitors, log,
          spec.planner, spec.runtime, spec.updateStrategy, CommunityRuntimeBuilder,
          CommunityRuntimeContextCreatorV3_3, CommunityRuntimeContextCreatorv3_5, LastCommittedTxIdProvider(graph))
    }

  override def create(spec: PlannerSpec_v3_5, config: CypherPlannerConfiguration): Cypher35Compiler[_,_] =
    (spec.planner, spec.runtime) match {
      case (CypherPlannerOption.rule, _) =>
        throw new InvalidArgumentException("The rule planner is no longer a valid planner option in Neo4j 3.4. If you need to use it, please select compatibility mode Cypher 3.1")
      case _ =>
        Cypher35Compiler(config, CompilerEngineDelegator.CLOCK, kernelMonitors, log,
                          spec.planner, spec.runtime, spec.updateStrategy, CommunityRuntimeBuilder,
                          CommunityRuntimeContextCreatorv3_5, LastCommittedTxIdProvider(graph))
    }
}

class CompilerLibrary(factory: CompilerFactory) {

  private val compilers = new CopyOnWriteHashMap[CompilerKey, Compiler]

  def selectCompiler(cypherVersion: CypherVersion,
                     cypherPlanner: CypherPlannerOption,
                     cypherRuntime: CypherRuntimeOption,
                     cypherUpdateStrategy: CypherUpdateStrategy,
                     config: CypherPlannerConfiguration): Compiler = {
    val key = CompilerKey(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy)
    val compiler = compilers.get(key)
    if (compiler == null) {
      compilers.put(key, factory.createCompiler(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy, config))
      compilers.get(key)
    } else compiler
  }

  case class CompilerKey(cypherVersion: CypherVersion,
                         cypherPlanner: CypherPlannerOption,
                         cypherRuntime: CypherRuntimeOption,
                         cypherUpdateStrategy: CypherUpdateStrategy)
}
