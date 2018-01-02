/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import java.nio.file.{Files, Path}
import java.util

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.EnterpriseGraphDatabase
import org.neo4j.graphdb.factory.{GraphDatabaseFactoryState, GraphDatabaseSettings}
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.GraphDatabaseAPI

trait EnterpriseGraphDatabaseTestSupport extends GraphDatabaseTestSupport {
  self: CypherFunSuite =>

  var dir: Path = null

  override protected def createGraphDatabase(): GraphDatabaseAPI = {
    val config = new util.HashMap[String, String]()
    config.put(GraphDatabaseSettings.pagecache_memory.name, "8M")
    dir = Files.createTempDirectory(getClass.getSimpleName)
    val state = new GraphDatabaseFactoryState()
    new EnterpriseGraphDatabase(dir.toFile, config, state.databaseDependencies())
  }

  override protected def stopTest() {
    super.stopTest()
    FileUtils.deletePathRecursively(dir)
  }
}
