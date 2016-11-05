package org.neo4j.cypher.internal.compatibility.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.{CypherCompilerConfiguration, CypherCompilerFactory}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

case class RuleCompatibility(graph: GraphDatabaseQueryService,
                             config: CypherCompilerConfiguration,
                             clock: Clock,
                             kernelMonitors: KernelMonitors,
                             kernelAPI: KernelAPI) extends Compatibility {
  protected val compiler = {
    val monitors = WrappedMonitors(kernelMonitors)
    CypherCompilerFactory.ruleBasedCompiler(graph, config, clock, monitors, rewriterSequencer, typeConversions)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
