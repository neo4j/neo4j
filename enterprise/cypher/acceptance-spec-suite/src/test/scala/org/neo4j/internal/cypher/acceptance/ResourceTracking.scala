/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.net.URL

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.interpreted.CSVResource
import org.neo4j.graphdb.{DependencyResolver, GraphDatabaseService}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.{GraphDatabaseQueryService, monitoring}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

trait ResourceTracking extends CypherFunSuite {

  var resourceMonitor: TrackingResourceMonitor = _

  def trackResources(graph: GraphDatabaseService): Unit = trackResources(graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver)

  def trackResources(graph: GraphDatabaseQueryService): Unit = trackResources(graph.getDependencyResolver)

  def trackResources(resolver: DependencyResolver): Unit = {
    val monitors = resolver.resolveDependency(classOf[monitoring.Monitors])
    resourceMonitor = TrackingResourceMonitor()
    monitors.addMonitorListener(resourceMonitor)
  }

  case class TrackingResourceMonitor() extends ResourceMonitor {

    private var traced: Map[URL, Int] = Map()
    private var closed: Map[URL, Int] = Map()

    override def trace(resource: AutoCloseable): Unit =
      resource match {
        case CSVResource(url, _) =>
          val currCount = traced.getOrElse(url, 0)
          traced += url -> (currCount + 1)
        case _ =>
      }

    override def close(resource: AutoCloseable): Unit =
      resource match {
        case CSVResource(url, _) =>
          val currCount = closed.getOrElse(url, 0)
          closed += url -> (currCount + 1)
        case _ =>
      }

    def assertClosedAndClear(expectedNumberOfCSVs: Int): Unit = {
      if (traced.size != closed.size)
        traced.keys should be(closed.keys)
      traced.size should be(expectedNumberOfCSVs)
      for ( (tracedUrl, tracedCount) <- traced ) {
        closed.contains(tracedUrl) should be(true)
        closed(tracedUrl) should be(tracedCount)
      }
      traced = Map()
      closed = Map()
    }
  }
}

