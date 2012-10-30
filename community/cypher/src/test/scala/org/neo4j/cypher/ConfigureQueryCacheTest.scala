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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.junit.Assert._
import java.util
import org.neo4j.graphdb.factory.GraphDatabaseSettings


class ConfigureQueryCacheTest extends GraphDatabaseTestBase with Assertions
{

  @Test
  def shouldBeAbleToChangeCacheSize()
  {
    // Given
    val config: util.Map[String, String] = graph.asInstanceOf[InternalAbstractGraphDatabase].getConfig.getParams
    config.put(GraphDatabaseSettings.query_cache_size.name(), "2");
    graph.asInstanceOf[InternalAbstractGraphDatabase].getConfig.applyChanges(config)

    val engine = new ExecutionEngine(graph);

    // When
    engine.prepare("START n=node(1) RETURN n")
    engine.prepare("START n=node(2) RETURN n")
    engine.prepare("START n=node(3) RETURN n")

    // Then
    assertTrue("Should have cached last query", engine.isPrepared("START n=node(3) RETURN n"))
    assertTrue("Should have cached second query", engine.isPrepared("START n=node(2) RETURN n"))
    assertFalse("Should not have cached first query", engine.isPrepared("START n=node(1) RETURN n"))
  }

}