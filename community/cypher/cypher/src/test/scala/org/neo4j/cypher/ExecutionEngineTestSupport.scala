/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.neo4j.cypher.internal.commons.{CypherTestSuite, CypherTestSupport}
import org.neo4j.cypher.internal.compiler.v2_1.RewindableExecutionResult
import org.neo4j.kernel.monitoring.Monitors


case class ExpectedException[T <: Throwable](e: T) {
  def messageContains(s: String) = assertThat(e.getMessage, containsString(s))
}

trait ExecutionEngineTestSupport extends CypherTestSupport {
  self: CypherTestSuite with GraphDatabaseTestSupport =>

  var engine: ExecutionEngine = null

  override protected def initTest() {
    super.initTest()
    engine = new ExecutionEngine(graph)
  }

  def execute(q: String, params: (String, Any)*): ExecutionResult =
    RewindableExecutionResult(engine.execute(q, params.toMap))

  def profile(q: String, params: (String, Any)*): ExecutionResult =
    RewindableExecutionResult(engine.profile(q, params.toMap))

  def runAndFail[T <: Throwable : Manifest](q: String): ExpectedException[T] =
    ExpectedException(intercept[T](execute(q)))

  def executeScalar[T](q: String, params: (String, Any)*):T = engine.execute(q, params.toMap).toList match {
    case m :: Nil =>
      if (m.size!=1)
        fail(s"expected scalar value: $m")
      else
        m.head._2.asInstanceOf[T]
    case _ => fail(s"expected to get a single row back")
  }

  protected def timeOutIn(length: Int, timeUnit: TimeUnit)(f: => Unit) {
    val future = Future {
      f
    }

    Await.result(future, Duration.apply(length, timeUnit))
  }
}
