package org.neo4j.cypher.internal.compatibility.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerConfiguration, CypherCompilerFactory}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

case class RuleCompatibility(graph: GraphDatabaseQueryService,
                             config: CypherCompilerConfiguration,
                             clock: Clock,
                             kernelMonitors: KernelMonitors,
                             kernelAPI: KernelAPI) extends Compatibility {
  protected val compiler = {
    val nodeManager = graph.getDependencyResolver.resolveDependency(classOf[NodeManager])
    val entityAccessor = new EntityAccessorWrapper(nodeManager)
    val monitors = new WrappedMonitors(kernelMonitors)
    val databaseService = graph.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService
    CypherCompilerFactory.ruleBasedCompiler(databaseService, entityAccessor, config, clock, monitors, rewriterSequencer)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
