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
package org.neo4j.cypher.internal.executionplan

import collection.Seq
import org.neo4j.cypher.internal.pipes.Pipe
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{Entity, ReturnItem, NodeById, Query}
import org.neo4j.graphdb.GraphDatabaseService
import org.scalatest.Assertions
import org.neo4j.cypher.InternalException
import actors.threadpool.{ExecutionException, TimeUnit, Executors}

class ExecutionPlanImplTest extends Assertions with Timed {
  @Test def should_not_go_into_never_ending_loop() {
    val q = Query.start(NodeById("x", 0)).returns(ReturnItem(Entity("x"), "x"))

    val exception = intercept[ExecutionException](timeoutAfter(1) {
      val epi = new FakeEPI(q, null)
      epi.execute(Map())
    })
    
    assertTrue(exception.getCause.isInstanceOf[InternalException])
  }
}

class FakeEPI(q: Query, gds: GraphDatabaseService) extends ExecutionPlanImpl(q, gds) {
  override lazy val builders = Seq(new BadBuilder)
}

// This is a builder that accepts everything, but changes nothing
// It's a never ending loop waiting to happen
class BadBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = (p,q)
  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = true
  def priority = 0
}

trait Timed {
  def timeoutAfter(timeout: Long)(codeToTest: => Unit) {
    val executor = Executors.newSingleThreadExecutor
    val future = executor.submit(new Runnable {
      def run = codeToTest
    })
    try {
      future.get(timeout, TimeUnit.SECONDS)
    }
    finally {
      executor.shutdown()
    }
  }
}