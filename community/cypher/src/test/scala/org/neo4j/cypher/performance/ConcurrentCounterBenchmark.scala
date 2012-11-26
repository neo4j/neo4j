/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.performance

import org.neo4j.performance.domain.benchmark.concurrent._
import org.neo4j.performance.PerformanceProfiler
import org.neo4j.performance.domain.Units
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.graphdb.factory.GraphDatabaseFactory

object Runner
{
  def main(args: Array[String])
  {
    PerformanceProfiler.runAndDumpReport(new ConcurrentCounterBenchmark)
  }
}

class ConcurrentCounterBenchmark extends ConcurrentBenchmark(16) {

  private var gdb:GraphDatabaseService = null
  private var cypher:ExecutionEngine = null

  override def setUp {
    super.setUp()

    gdb = new GraphDatabaseFactory().newEmbeddedDatabase("target/test-data/congested-perf-graph.db")

    cypher = new ExecutionEngine(gdb)
    cypher.execute("START n=node(0) SET n.count = 0")
  }

  override def tearDown {
    gdb.shutdown()
  }

  def createWorker = new SimpleBenchmarkWorker() {
    def runOperation {
      cypher.execute("START n=node(0) SET n.count = n.count + 1")
    }
  }
}