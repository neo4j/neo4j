/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import scala.collection.immutable.Seq

class MergeConcurrencyIT extends ExecutionEngineJUnitSuite {

  val nodeCount = 100
  val threadCount = Runtime.getRuntime.availableProcessors() * 2

  @Test def should_handle_ten_simultaneous_threads() {
      // Given a constraint on :Label(id), create a linked list
      execute("CREATE CONSTRAINT ON (n:Label) ASSERT n.id IS UNIQUE")

      val runner = new Runnable {
        def run() {
          (1 to nodeCount) foreach {
            x =>
              execute("MERGE (a:Label {id:{id}}) " +
                "MERGE (b:Label {id:{id}+1}) " +
                "MERGE (a)-[r:TYPE]->(b) " +
                "RETURN a, b, r", "id" -> x)
          }
        }
      }

      val threads: Seq[Thread] = (0 to threadCount-1) map (x => new Thread(runner))

      threads.foreach(_.start())
      threads.foreach(_.join())

      // Now check that the list exists and is a single one:
      val result = execute(s"match p=(:Label {id:1})-[*..1000]->({id:$nodeCount}) return 1")
      assert(result.size === 1)
  }
}
