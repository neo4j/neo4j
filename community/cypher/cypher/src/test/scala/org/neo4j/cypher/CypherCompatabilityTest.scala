/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test
import org.neo4j.test.ImpermanentGraphDatabase
import collection.JavaConverters._
import java.util

class CypherCompatabilityTest {
  @Test
  def should_accept_1_9_queries_with_db_config() {
    // given
    val java: util.Map[String, String] = Map("cypher_parser_version" -> "1.9").asJava

    val graph = new ImpermanentGraphDatabase(java) with Snitch
    try {
      val engine = new ExecutionEngine(graph)

      // then doesn't throw
      engine.execute("create (n) return n.prop?").dumpToString()
    } finally {
      graph.shutdown()
    }
  }

  @Test
  def should_accept_1_9_queries_using_query_prologue() {
    // given
    val graph = new ImpermanentGraphDatabase() with Snitch
    try {
      val engine = new ExecutionEngine(graph)

      // then doesn't throw
      engine.execute("cypher 1.9 create (n) return n.prop?").dumpToString()
    } finally {
      graph.shutdown()
    }
  }
}