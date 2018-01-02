/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.{CypherFunSuite, CypherTestSupport}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


case class ExpectedException[T <: Throwable](e: T) {
  def messageContains(s: String) = assertThat(e.getMessage, containsString(s))
}

trait ExecutionEngineTestSupport extends CypherTestSupport {
  self: CypherFunSuite with GraphDatabaseTestSupport =>

  var eengine: ExecutionEngine = null

  override protected def initTest() {
    super.initTest()
    eengine = new ExecutionEngine(graph)
  }

  def execute(q: String, params: (String, Any)*): InternalExecutionResult =
    RewindableExecutionResult(eengine.execute(q, params.toMap))

  def profile(q: String, params: (String, Any)*): InternalExecutionResult =
    RewindableExecutionResult(eengine.profile(q, params.toMap))

  def runAndFail[T <: Throwable : Manifest](q: String): ExpectedException[T] =
    ExpectedException(intercept[T](execute(q)))

  def executeScalar[T](q: String, params: (String, Any)*): T = scalar(eengine.execute(q, params.toMap).toList)

  def scalar[T](input: List[Map[String, Any]]) = input match {
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
