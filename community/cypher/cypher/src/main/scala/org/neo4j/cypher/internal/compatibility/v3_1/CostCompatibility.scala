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
package org.neo4j.cypher.internal.compatibility.v3_1

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Id
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

case class CostCompatibility(graph: GraphDatabaseQueryService,
                             config: CypherCompilerConfiguration,
                             clock: Clock,
                             kernelMonitors: KernelMonitors,
                             log: Log,
                             planner: CypherPlanner,
                             runtime: CypherRuntime,
                             strategy: CypherUpdateStrategy) extends Compatibility {

  protected val compiler = {
    val plannerName = planner match {
      case CypherPlanner.default => None
      case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerName)
      case CypherPlanner.dp => Some(DPPlannerName)
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
    }

    val runtimeName = runtime match {
      case CypherRuntime.default => None
      case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
      case CypherRuntime.compiled => Some(CompiledRuntimeName)
      case _ => throw new IllegalArgumentException("Runtime is not supported in Cypher 3.1")
    }
    val updateStrategy = strategy match {
      case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
      case _ => None
    }

    val logger = new StringInfoLogger(log)
    val monitors = WrappedMonitors(kernelMonitors)
    CypherCompilerFactory.costBasedCompiler(graph, config, clock, NOT_SUPPORTED, monitors, logger,
      rewriterSequencer, plannerName, runtimeName, updateStrategy, typeConversions)
  }

  override val queryCacheSize: Int = config.queryCacheSize

  private object NOT_SUPPORTED extends CodeStructure[GeneratedQuery] {

    override def generateQuery(className: String, columns: Seq[String],
                               operatorIds: Map[String, Id],
                               conf: CodeGenConfiguration)
                              (block: (MethodStructure[_]) => Unit)
                              (implicit
                               codeGenContext: CodeGenContext): CodeStructureResult[GeneratedQuery] =
      throw new IllegalArgumentException("Runtime is not supported in Cypher 3.1")
  }
}
