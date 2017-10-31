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
package org.neo4j.cypher.internal.compatibility.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.{CypherCompilerConfiguration, CypherCompilerFactory}
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
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
