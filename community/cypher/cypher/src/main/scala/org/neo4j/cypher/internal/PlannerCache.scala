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

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v3_1.CypherCompilerConfiguration
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.collection.mutable

sealed trait PlannerSpec
final case class PlannerSpec_v2_3(planner: CypherPlanner, runtime: CypherRuntime) extends PlannerSpec
final case class PlannerSpec_v3_0(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec
final case class PlannerSpec_v3_1(planner: CypherPlanner, runtime: CypherRuntime, updateStrategy: CypherUpdateStrategy) extends PlannerSpec

class PlannerFactory(graph: GraphDatabaseQueryService, kernelAPI: KernelAPI, kernelMonitors: KernelMonitors, log: Log,
                     config: CypherCompilerConfiguration) {

  import helpers.wrappersFor2_3._
  import helpers.wrappersFor3_0._

  def create(spec: PlannerSpec_v2_3) =  spec.planner match {
    case CypherPlanner.rule => CompatibilityFor2_3Rule(graph, as2_3(config), Clock.SYSTEM_CLOCK, kernelMonitors, kernelAPI)
    case _ => CompatibilityFor2_3Cost(graph, as2_3(config),
                                      Clock.SYSTEM_CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime)
  }

  def create(spec: PlannerSpec_v3_0) =  spec.planner match {
    case CypherPlanner.rule => CompatibilityFor3_0Rule(graph, as3_0(config),CypherCompiler.CLOCK, kernelMonitors, kernelAPI)
    case _ => CompatibilityFor3_0Cost(graph, as3_0(config),
                                      CypherCompiler.CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime, spec.updateStrategy)
  }

  def create(spec: PlannerSpec_v3_1) = spec.planner match {
    case CypherPlanner.rule => CompatibilityFor3_1Rule(graph, config, CypherCompiler.CLOCK, kernelMonitors, kernelAPI)
    case _ => CompatibilityFor3_1Cost(graph, config,
                                      CypherCompiler.CLOCK, kernelMonitors, kernelAPI, log, spec.planner, spec.runtime,
                                      spec.updateStrategy)
  }
}

class PlannerCache(factory: PlannerFactory)  {
  private val cache_v2_3 = new mutable.HashMap[PlannerSpec_v2_3, CompatibilityFor2_3]
  private val cache_v3_0 = new mutable.HashMap[PlannerSpec_v3_0, CompatibilityFor3_0]
  private val cache_v3_1 = new mutable.HashMap[PlannerSpec_v3_1, CompatibilityFor3_1]

  def apply(spec: PlannerSpec_v2_3) = cache_v2_3.getOrElseUpdate(spec, factory.create(spec))
  def apply(spec: PlannerSpec_v3_0) = cache_v3_0.getOrElseUpdate(spec, factory.create(spec))
  def apply(spec: PlannerSpec_v3_1) = cache_v3_1.getOrElseUpdate(spec, factory.create(spec))
}

class CachingValue[T]() {
  private var innerOption: Option[T] = None

  def getOrElseUpdate(op: => T) = innerOption match {
    case None =>
      innerOption = Some(op)
      innerOption.get
    case Some(value) => value
  }
}
