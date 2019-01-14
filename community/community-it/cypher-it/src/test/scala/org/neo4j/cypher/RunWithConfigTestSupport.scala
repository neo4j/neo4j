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
package org.neo4j.cypher

import java.io.File
import java.util

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.JavaConverters._

trait RunWithConfigTestSupport {
  def runWithConfig(m: (Setting[_], String)*)(run: GraphDatabaseCypherService => Unit) = {
    val config: util.Map[Setting[_], String] = m.toMap.asJava
    val storeDir = new File("target/test-data/impermanent-custom-config")
    val graph = new TestGraphDatabaseFactory().newImpermanentDatabase(storeDir, config)
    try {
      run(new GraphDatabaseCypherService(graph))
    } finally {
      graph.shutdown()
    }
  }
}
