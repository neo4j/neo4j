package org.neo4j.cypher.internal.compatibility.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.spi.v3_2.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

case class CostCompatibility(graph: GraphDatabaseQueryService,
                             config: CypherCompilerConfiguration,
                             clock: Clock,
                             kernelMonitors: KernelMonitors,
                             kernelAPI: KernelAPI,
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
    }
    val updateStrategy = strategy match {
      case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
      case _ => None
    }

    val logger = new StringInfoLogger(log)
    val monitors = WrappedMonitors(kernelMonitors)
    CypherCompilerFactory.costBasedCompiler(graph, config, clock, GeneratedQueryStructure, monitors, logger,
      rewriterSequencer, plannerName, runtimeName, updateStrategy, typeConversions)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
