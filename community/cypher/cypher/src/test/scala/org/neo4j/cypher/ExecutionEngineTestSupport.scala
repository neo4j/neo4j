/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.{ExecutionEngine, RewindableExecutionResult}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.kernel.impl.query.TransactionalContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class ExpectedException[T <: Throwable](e: T) {
  def messageContains(s: String) = assertThat(e.getMessage, containsString(s))
}

trait ExecutionEngineTestSupport extends CypherTestSupport with ExecutionEngineHelper {
  self: CypherFunSuite with GraphDatabaseTestSupport =>

  var eengine: ExecutionEngine = null

  override protected def initTest() {
    super.initTest()
    eengine = new ExecutionEngine(graph)
  }

  def runAndFail[T <: Throwable : Manifest](q: String): ExpectedException[T] =
    ExpectedException(intercept[T](execute(q)))

  override def executeScalar[T](q: String, params: (String, Any)*): T = try {
    super.executeScalar[T](q, params: _*)
  } catch {
    case e: ScalarFailureException => fail(e.getMessage)
  }

  protected def timeOutIn(length: Int, timeUnit: TimeUnit)(f: => Unit) {
    val future = Future {
      f
    }

    Await.result(future, Duration.apply(length, timeUnit))
  }
}

trait ExecutionEngineHelper {
  self: GraphIcing =>

  def graph: GraphDatabaseCypherService

  def eengine: ExecutionEngine

  def execute(q: String, params: (String, Any)*): InternalExecutionResult =
    RewindableExecutionResult(eengine.execute(q, params.toMap, graph.transactionalContext(query = q -> params.toMap)))

  def profile(q: String, params: (String, Any)*): InternalExecutionResult =
    RewindableExecutionResult(eengine.profile(q, params.toMap, graph.transactionalContext(query = q -> params.toMap)))

  def executeScalar[T](q: String, params: (String, Any)*): T =
    scalar[T](eengine.execute(q, params.toMap, graph.transactionalContext(query = q -> params.toMap)).toList)

  private def scalar[T](input: List[Map[String, Any]]): T = input match {
    case m :: Nil =>
      if (m.size != 1)
        throw new ScalarFailureException(s"expected scalar value: $m")
      else {
        val value: Any = m.head._2
        value.asInstanceOf[T]
      }
    case _ => throw new ScalarFailureException(s"expected to get a single row back")
  }

  protected class ScalarFailureException(msg: String) extends RuntimeException(msg)

  implicit class RichExecutionEngine(engine: ExecutionEngine) {
    def profile(query: String, params: Map[String, Any]) =
      engine.profile(query, params, engine.queryService.transactionalContext(query = query -> params))

    def execute(query: String, params: Map[String, Any]) =
      engine.execute(query, params, engine.queryService.transactionalContext(query = query -> params))
  }
}
